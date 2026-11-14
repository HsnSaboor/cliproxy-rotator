package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// probeTimeout bounds one exit probe. It is deliberately short: a dead proxy
// must be dropped from the pool long before it can slow a request down.
const probeTimeout = 6 * time.Second

// probeConcurrency is how many exits are probed at once.
const probeConcurrency = 8

// probeAll checks every SOCKS/HTTP exit against target and parks the ones
// that cannot reach it.
//
// The live pool file mixes working multiplexer users with raw proxies that
// died minutes ago. Without this probe the plugin would only learn that a
// proxy is dead by paying its dial timeout inside a real request.
func (p *egressPool) probeAll(target string) {
	p.socksMu.Lock()
	entries := append([]*egressEntry(nil), p.socks...)
	p.socksMu.Unlock()
	p.probeEntries(target, entries)
}

// probeFresh checks only the exits that have never been measured. It is used
// after the pool file changed: exits that already answered a request are
// trusted until they fail, and exits that already failed stay parked.
func (p *egressPool) probeFresh(target string, candidates []*egressEntry) {
	fresh := make([]*egressEntry, 0, len(candidates))
	for _, entry := range candidates {
		if entry == nil {
			continue
		}
		if entry.samples.Load() == 0 && entry.failures.Load() == 0 {
			fresh = append(fresh, entry)
		}
	}
	p.probeEntries(target, fresh)
}

// probeEntries runs one probe round. Only one round runs at a time: the host
// re-applies the plugin configuration several times during startup, and five
// overlapping rounds would hammer the proxy pool for no benefit.
func (p *egressPool) probeEntries(target string, entries []*egressEntry) {
	if target == "" || len(entries) == 0 {
		return
	}
	if !p.probing.CompareAndSwap(false, true) {
		return
	}
	defer p.probing.Store(false)

	debug := socketDebug() != ""
	if debug {
		fmt.Fprintf(os.Stderr, "cliproxy-rotator probe: %d exit(s) against %s\n", len(entries), target)
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, probeConcurrency)
	for _, entry := range entries {
		wg.Add(1)
		go func(e *egressEntry) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := probeProxyExit(e, target); err != nil {
				if debug {
					fmt.Fprintf(os.Stderr, "cliproxy-rotator probe: %s dead: %v\n", e.label, err)
				}
				e.parkDead(err.Error())
				return
			}
			e.succeed()
		}(entry)
	}
	wg.Wait()

	alive := 0
	for _, entry := range entries {
		if entry.usable() {
			alive++
		}
	}
	message := fmt.Sprintf("cliproxy-rotator: probed %d proxy exit(s) against %s: %d reachable",
		len(entries), target, alive)
	hostLog("info", message)
	// The host keeps its own log in a file once the configuration is applied,
	// so mirror the summary on stderr to keep it visible in the journal.
	fmt.Fprintln(os.Stderr, message)
}

// probeProxyExit dials the proxy and asks it to connect to target.
func probeProxyExit(entry *egressEntry, target string) error {
	if entry == nil || entry.socks == nil {
		return nil
	}
	proxyAddr := entry.socks.Host
	if entry.socks.Port() == "" {
		proxyAddr = net.JoinHostPort(entry.socks.Hostname(), "1080")
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	dialer := &net.Dialer{Timeout: probeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return fmt.Errorf("dial proxy %s: %w", proxyAddr, err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(probeTimeout))

	host, portText, errSplit := net.SplitHostPort(target)
	if errSplit != nil {
		return fmt.Errorf("split target %q: %w", target, errSplit)
	}
	port, errPort := strconv.Atoi(portText)
	if errPort != nil {
		return fmt.Errorf("parse target port %q: %w", portText, errPort)
	}

	switch strings.ToLower(entry.socks.Scheme) {
	case "http", "https":
		return probeHTTPConnect(conn, target)
	default:
		return probeSocks5Connect(conn, host, port, entry.socks.User.Username(), passwordOf(entry.socks))
	}
}

// passwordOf returns the password of a proxy URL.
func passwordOf(u *url.URL) string {
	if u == nil {
		return ""
	}
	password, _ := u.User.Password()
	return password
}

// probeHTTPConnect asks an HTTP proxy for a CONNECT tunnel.
func probeHTTPConnect(conn net.Conn, target string) error {
	request := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Connection: keep-alive\r\n\r\n", target, target)
	if _, err := conn.Write([]byte(request)); err != nil {
		return fmt.Errorf("http proxy connect: %w", err)
	}
	buffer := make([]byte, 256)
	n, err := conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("http proxy reply: %w", err)
	}
	status := string(buffer[:n])
	if !strings.Contains(status, " 200") {
		return fmt.Errorf("http proxy refused CONNECT: %s", strings.TrimSpace(strings.SplitN(status, "\r\n", 2)[0]))
	}
	return nil
}

// probeSocks5Connect performs the SOCKS5 handshake and asks for a tunnel.
func probeSocks5Connect(conn net.Conn, host string, port int, username, password string) error {
	methods := []byte{0x05, 0x01, 0x00}
	if username != "" {
		methods = []byte{0x05, 0x02, 0x00, 0x02}
	}
	if _, err := conn.Write(methods); err != nil {
		return fmt.Errorf("socks5 greeting: %w", err)
	}
	greeting := make([]byte, 2)
	if _, err := readFull(conn, greeting); err != nil {
		return fmt.Errorf("socks5 greeting reply: %w", err)
	}
	if greeting[0] != 0x05 {
		return fmt.Errorf("socks5: unexpected version %d", greeting[0])
	}
	switch greeting[1] {
	case 0x00:
	case 0x02:
		if username == "" {
			return fmt.Errorf("socks5: proxy demands credentials")
		}
		auth := []byte{0x01, byte(len(username))}
		auth = append(auth, username...)
		auth = append(auth, byte(len(password)))
		auth = append(auth, password...)
		if _, err := conn.Write(auth); err != nil {
			return fmt.Errorf("socks5 auth: %w", err)
		}
		reply := make([]byte, 2)
		if _, err := readFull(conn, reply); err != nil {
			return fmt.Errorf("socks5 auth reply: %w", err)
		}
		if reply[1] != 0x00 {
			return fmt.Errorf("socks5 authentication rejected")
		}
	default:
		return fmt.Errorf("socks5: no acceptable authentication method")
	}

	request := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if ipv4 := ip.To4(); ipv4 != nil {
			request = append(request, 0x01)
			request = append(request, ipv4...)
		} else {
			request = append(request, 0x04)
			request = append(request, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return fmt.Errorf("socks5: hostname too long")
		}
		request = append(request, 0x03, byte(len(host)))
		request = append(request, host...)
	}
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, uint16(port))
	request = append(request, portBytes...)
	if _, err := conn.Write(request); err != nil {
		return fmt.Errorf("socks5 connect: %w", err)
	}
	header := make([]byte, 4)
	if _, err := readFull(conn, header); err != nil {
		return fmt.Errorf("socks5 connect reply: %w", err)
	}
	if header[1] != 0x00 {
		return fmt.Errorf("socks5 connect failed with code %d", header[1])
	}
	return nil
}

// readFull reads exactly len(buffer) bytes.
func readFull(conn net.Conn, buffer []byte) (int, error) {
	total := 0
	for total < len(buffer) {
		n, err := conn.Read(buffer[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}
