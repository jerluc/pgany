VERSION := `git describe --tags 2>/dev/null || echo "untagged"`
COMMITISH := `git describe --always 2>/dev/null`

deps:
	go mod tidy
	go mod verify

format:
	go fmt ./...

build: deps
	go build -o target/pgany -ldflags="-X main.Version=${VERSION} -X main.Commitish=${COMMITISH}" ./cmd/...

lint: format
	golint github.com/jerluc/pgany

test: build
	go test -v -coverprofile=/tmp/pgany.coverage.out ./...

coverage: test
	go tool cover -func=/tmp/pgany.coverage.out

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

.PHONY: deps format build lint test coverage tools
