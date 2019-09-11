GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
PACKAGENAME=virgo4-marc-ingest
BINNAME=$(PACKAGENAME)

build: darwin 

all: darwin linux

darwin:
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -a -o bin/$(BINNAME).darwin cmd/$(PACKAGENAME)/*.go

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -a -installsuffix cgo -o bin/$(BINNAME).linux cmd/$(PACKAGENAME)/*.go

clean:
	$(GOCLEAN) cmd/
	rm -rf bin
