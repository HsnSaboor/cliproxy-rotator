PLUGIN := cliproxy-rotator.so
GOARCH := $(shell go env GOARCH)

ifeq ($(GOARCH),amd64)
CGO_CFLAGS_PLUGIN := -march=x86-64
else ifeq ($(GOARCH),arm64)
CGO_CFLAGS_PLUGIN := -march=armv8-a
endif

.PHONY: build test install clean

build:
	CGO_ENABLED=1 CGO_CFLAGS="$(CGO_CFLAGS_PLUGIN)" go build -buildmode=c-shared -trimpath -o $(PLUGIN) .
	@rm -f cliproxy-rotator.h

test:
	go test ./...

clean:
	rm -f $(PLUGIN) cliproxy-rotator.h

PLUGIN_DIR ?= $(HOME)/.cache/cliproxy-plugins

install: build
	install -d $(PLUGIN_DIR)
	install -m 0755 $(PLUGIN) $(PLUGIN_DIR)/$(PLUGIN)
	install -d $(PLUGIN_DIR)/proxies
	install -m 0644 proxies/*.txt proxies/*.json $(PLUGIN_DIR)/proxies/

reload:
	systemctl --user restart cli-proxy-api.service
