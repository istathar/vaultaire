all: build

.PHONY: all build test

build:
	@/bin/echo -e "CABAL\tbuild"
	cabal build

test: dist/setup-config
	@/bin/echo -e "CABAL\ttest"
	cabal test

dist/setup-config:
	@/bin/echo -e "CABAL\tinstall --only-dependencies"
	cabal install --only-dependencies --enable-tests --enable-benchmarks

%:
	cabal build $@

ifdef V
MAKEFLAGS=-R
else
MAKEFLAGS=-s -R
REDIRECT=2>/dev/null
endif


SOURCES=$(shell find src -name '*.hs' -type f) \
	$(shell find tests -name '*.hs' -type f) \
	$(shell find lib -name '*.hs' -type f)

HOTHASKTAGS=$(shell which hothasktags 2>/dev/null)
CTAGS=$(if $(HOTHASKTAGS),$(HOTHASKTAGS),/bin/false)

tags: $(SOURCES)
	if [ "$(HOTHASKTAGS)" ] ; then /bin/echo -e "CTAGS\ttags" ; fi
	-$(CTAGS) $^ > tags $(REDIRECT)

