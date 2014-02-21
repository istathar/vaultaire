check_vaultaire: install

install: build check
	go install

build: deps
	go build

deps:
	go get

clean:
	rm -f check_vaultaire

check:
	go test
