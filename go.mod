module github.com/arangodb-managed/arangocopy

go 1.13

require (
	github.com/arangodb/go-driver v0.0.0-20200107125107-2a2392e62f69
	github.com/briandowns/spinner v1.11.1
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/coreos/go-semver v0.3.0
	github.com/rs/zerolog v1.14.3
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	golang.org/x/crypto v0.0.0-20190308221718-c2843e01d9a2
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
)

replace github.com/arangodb/go-driver => /Users/skarlso/arangodb/go-driver
