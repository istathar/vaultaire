all: tags build

#
# Top-level targets. This is ugly. A program to extract these from the .cabal
# file would work, but is there anything easier?
#

marquised: dist/build/marquised/marquised
reader-test: dist/build/reader-test/reader-test
reader-algorithms: dist/build/reader-algorithms/reader-algorithms
writer-test: dist/build/writer-test/writer-test
daymap-test: dist/build/daymap-test/daymap-test
writer-test: dist/build/writer-test/writer-test
identity-test: dist/build/identity-test/identity-test

#
# Setup
#

ifdef V
MAKEFLAGS=-R
else
MAKEFLAGS=-s -R
REDIRECT=2>/dev/null
endif

.PHONY: all build test

#
# Build rules. This just wraps Cabal doing its thing in a Haskell
# language-specific fashion.
#

build:
	@/bin/echo -e "CABAL\tbuild"
	cabal build

test: dist/setup-config tags
	@/bin/echo -e "CABAL\ttest"
	cabal test

dist/setup-config:
	@/bin/echo -e "CABAL\tinstall --only-dependencies"
	cabal install --only-dependencies --enable-tests --enable-benchmarks
	@/bin/echo -e "CABAL\tconfigure"
	cabal configure --enable-tests --enable-benchmarks


# This will match writer-test/writer-test, so we have to strip the directory
# portion off. Annoying, but you can't use two '%' in a pattern rule.
dist/build/%: dist/setup-config $(SOURCES)
	@/bin/echo -e "CABAL\tbuild $@"
	cabal build $(notdir $@)

#
# Build ctags file
#

SOURCES=$(shell find src -name '*.hs' -type f) \
	$(shell find tests -name '*.hs' -type f) \
	$(shell find lib -name '*.hs' -type f)

HOTHASKTAGS=$(shell which hothasktags 2>/dev/null)
CTAGS=$(if $(HOTHASKTAGS),$(HOTHASKTAGS),/bin/false)

tags: $(SOURCES)
	if [ "$(HOTHASKTAGS)" ] ; then /bin/echo -e "CTAGS\ttags" ; fi
	-$(CTAGS) $^ > tags $(REDIRECT)
