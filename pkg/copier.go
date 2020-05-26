//
// DISCLAIMER
//
// Copyright 2020 ArangoDB GmbH, Cologne, Germany
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright holder is ArangoDB GmbH, Cologne, Germany
//
// Author Gergely Brautigam
//

package pkg

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/cenkalti/backoff"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/rs/zerolog"
)

var (
	backoffMaxTries = 5
)

// Copier copies database content from source address to destination address
type Copier interface {
	// Copy copies over every database, collection, view, etc, from source to destination.
	// At the destination everything will be overwritten.
	Copy() error
}

// Config defines configuration for this copier.
type Config struct {
	SourceAddress       string
	SourceUsername      string
	SourcePassword      string
	DestinationAddress  string
	DestinationUsername string
	DestinationPassword string
	IncludedDatabases   []string
	ExcludedDatabases   []string
	IncludedCollections []string
	ExcludedCollections []string
	IncludedViews       []string
	ExcludedViews       []string
	Force               bool
	Parallel            int
	Timeout             float64
	BatchSize           int
}

// Dependencies defines dependencies for the copier.
type Dependencies struct {
	Logger zerolog.Logger
}

type copier struct {
	Config
	Dependencies
	sourceClient      driver.Client
	destinationClient driver.Client
	databaseInclude   map[string]struct{}
	databaseExclude   map[string]struct{}
	collectionInclude map[string]struct{}
	collectionExclude map[string]struct{}
	viewInclude       map[string]struct{}
	viewExclude       map[string]struct{}
}

// NewCopier returns a new copier with given a given set of configurations.
func NewCopier(cfg Config, deps Dependencies) (Copier, error) {
	c := &copier{
		Config:       cfg,
		Dependencies: deps,
	}
	// Set up source client.
	if client, err := c.getClient("Source", cfg.SourceAddress, cfg.SourceUsername, cfg.SourcePassword); err != nil {
		c.Logger.Error().Err(err).Msg("Failed to connect to source address.")
		return nil, err
	} else {
		c.sourceClient = client
	}
	// Set up destination client.
	if client, err := c.getClient("Destination", cfg.DestinationAddress, cfg.DestinationUsername, cfg.DestinationPassword); err != nil {
		c.Logger.Error().Err(err).Msg("Failed to connect to destination address.")
		return nil, err
	} else {
		c.destinationClient = client
	}
	// Set up filters
	c.databaseInclude = setupMap(c.Config.IncludedDatabases)
	c.databaseExclude = setupMap(c.Config.ExcludedDatabases)
	c.collectionInclude = setupMap(c.Config.IncludedCollections)
	c.collectionExclude = setupMap(c.Config.ExcludedCollections)
	c.viewInclude = setupMap(c.Config.IncludedViews)
	c.viewExclude = setupMap(c.Config.ExcludedViews)
	return c, nil
}

// A small helper to setup a map for filters.
func setupMap(data []string) map[string]struct{} {
	m := make(map[string]struct{})
	for _, f := range data {
		m[f] = struct{}{}
	}
	return m
}

// getClient creates a client pointing to address and tests if that connection works.
func (c *copier) getClient(prefix, address, username, password string) (driver.Client, error) {
	log := c.Logger
	// Open a connection
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{address},
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to open conncetion to address")
		return nil, err
	}
	// Create the client
	cfg := driver.ClientConfig{
		Connection: conn,
	}
	if username != "" && password != "" {
		cfg.Authentication = driver.BasicAuthentication(username, password)
	}
	client, err := driver.NewClient(cfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create driver client")
		return nil, err
	}
	// Test a connection to the database
	version, err := client.Version(context.Background())
	if err != nil {
		log.Error().Err(err).Msg("Failed to connect to database")
		return nil, err
	}
	log.Info().Msgf("%s: Version at address (%s) is %s", prefix, address, version.String())
	return client, nil
}

// Copy copies over every database, collection, view, etc, from source to destination.
// At the destination everything will be overwritten.
func (c *copier) Copy() error {
	log := c.Logger

	if !c.Force {
		var response string
		fmt.Print("Please confirm copy operation (y/N) ")
		fmt.Scanln(&response)
		if response != "y" {
			log.Info().Msg("Halting operation.")
			return nil
		}
	} else {
		log.Info().Msg("Force is being used, confirm skipped.")
	}

	ctx := context.Background()
	// Gather all databases
	var sourceDbs []driver.Database
	c.backoffCall(ctx, func() error {
		dbs, err := c.sourceClient.Databases(ctx)
		if err != nil {
			c.Logger.Error().Err(err).Msg("Failed to get databases for source.")
			return err
		}
		sourceDbs = dbs
		return nil
	})

	databases := c.filterDatabases(sourceDbs)

	for _, db := range databases {
		if err := c.copyDatabase(ctx, db); err != nil {
			return err
		}
		log.Info().Msg("Done with databases.")
		if err := c.copyCollections(ctx, db); err != nil {
			return err
		}
		log.Info().Msg("Done with collections.")
		if err := c.copyViews(ctx, db); err != nil {
			return err
		}
		log.Info().Msg("Done with viewes.")
	}

	return nil
}

// backoffCall is a convenient wrapper around backoff Retry.
func (c *copier) backoffCall(ctx context.Context, f func() error) {
	if err := backoff.Retry(f, backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(backoffMaxTries)), ctx)); err != nil {
		c.Logger.Fatal().Err(err).Msg("Backoff eventually failed.")
	}
}
