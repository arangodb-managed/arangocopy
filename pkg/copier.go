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
	"time"

	"github.com/cenkalti/backoff"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/rs/zerolog"
)

var (
	// cursorTimeout needs to be a high value, otherwise the cursor disappears
	// when a long read is being performed
	cursorTimeout   = 2 * time.Hour
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
	log.Debug().Msgf("%s: Version at address (%s) is %s", prefix, address, version.String())
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
	sourceDbs, err := c.sourceClient.Databases(ctx)
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to get databases for source.")
		return err
	}

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
		if err := c.copyIndexes(ctx, db); err != nil {
			return err
		}
		log.Info().Msg("Done with indexes.")

		if err := c.copyViews(ctx, db); err != nil {
			return err
		}
		log.Info().Msg("Done with viewes.")
	}

	return nil
}

// copyCollections copies all collections for a database.
func (c *copier) copyCollections(ctx context.Context, db driver.Database) error {
	log := c.Logger

	sourceDB, err := c.sourceClient.Database(ctx, db.Name())
	if err != nil {
		return err
	}
	list, err := sourceDB.Collections(ctx)
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to list collections for source database.")
		return err
	}
	collections := c.filterCollections(list)

	// TODO: This actually causes an issue where an ongoing, working query just throws a timeout and the cursor dies
	// immediately without the possibility to retry the operation.
	//timeout := driver.WithQueryMaxRuntime(ctx, c.Timeout)
	destinationDB, err := c.destinationClient.Database(ctx, sourceDB.Name())
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to ensure destination databases.")
		return nil
	}
	readCtx := driver.WithQueryStream(ctx, true)
	readCtx = driver.WithQueryBatchSize(readCtx, c.BatchSize)
	restoreCtx := driver.WithIsRestore(ctx, true)
	var g errgroup.Group
	sem := semaphore.NewWeighted(int64(c.Parallel))
	for _, sourceColl := range collections {
		sourceColl := sourceColl
		g.Go(func() error {
			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}
			defer sem.Release(1)
			sourceProps, err := sourceColl.Properties(ctx)
			if err != nil {
				c.Logger.Error().Err(err).Msg("Failed to get properties.")
				return err
			}
			if sourceProps.IsSystem {
				// skip system collections
				return nil
			}
			var destinationColl driver.Collection
			if err := backoff.Retry(func() error {
				dColl, err := c.ensureDestinationCollection(ctx, destinationDB, sourceColl, sourceProps)
				if err != nil {
					c.Logger.Error().Err(err).Msg("Failed to ensure destination collection.")
					return err
				}
				destinationColl = dColl
				return nil
			}, backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(backoffMaxTries)), ctx)); err != nil {
				c.Logger.Error().Err(err).Msg("Backoff eventually failed.")
				return err
			}
			log.Debug().Str("source-collection", sourceColl.Name()).Msg("Commencing data copy for collection...")
			bindVars := map[string]interface{}{
				"@c": sourceColl.Name(),
			}
			cursor, err := sourceDB.Query(readCtx, "FOR d IN @@c RETURN d", bindVars)
			if err != nil {
				c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Msg("Failed to query source database for collection.")
				return err
			}
			batch := make([]interface{}, c.BatchSize)
			for {
				var d interface{}
				if err := backoff.Retry(func() error {
					if _, err := cursor.ReadDocument(readCtx, &d); err != nil {
						c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Msg("Read documents failed.")
						return err
					}
					batch = append(batch, d)
					return nil
				}, backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(backoffMaxTries)), ctx)); err != nil {
					c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Msg("Backoff eventually failed.")
					cursor.Close()
					return err
				}

				if (!cursor.HasMore() && len(batch) > 0) || len(batch) >= c.BatchSize {
					if err := backoff.Retry(func() error {
						if _, _, err := destinationColl.CreateDocuments(restoreCtx, batch); err != nil {
							c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Interface("document", d).Msg("Creating a document failed.")
							return err
						}
						batch = make([]interface{}, c.BatchSize)
						return nil
					}, backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(backoffMaxTries)), ctx)); err != nil {
						c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Msg("Backoff eventually failed.")
						cursor.Close()
						return err
					}
				}

				if !cursor.HasMore() {
					break
				}
			}
			fmt.Println()
			log.Debug().Str("collection", sourceColl.Name()).Msg("Done reader for collection.")
			cursor.Close()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Error().Err(err).Msg("One of the workers failed to copy data.")
		return err
	}
	log.Debug().Str("source-database", sourceDB.Name()).Msg("Done copying database data.")
	return nil
}

// copyViews copies all views from source database to destination database.
func (c *copier) copyViews(ctx context.Context, db driver.Database) error {
	log := c.Logger
	// Get the destination database
	destinationDb, err := c.destinationClient.Database(ctx, db.Name())
	if err != nil {
		log.Error().Err(err).Msg("Failed to get destination database")
		return err
	}
	sourceViews, err := db.Views(ctx)
	views := c.filterViews(sourceViews)
	if err != nil {
		log.Error().Err(err).Msg("Failed to find all views.")
		return err
	}
	for _, v := range views {
		log = log.With().Str("view", v.Name()).Str("db", db.Name()).Logger()
		// Check if view already exists
		if ok, err := destinationDb.ViewExists(ctx, v.Name()); err != nil {
			log.Error().Err(err).Msg("Error checking if view exists.")
			return err
		} else if ok {
			// skip if it exists
			continue
		}

		asv, err := v.ArangoSearchView()
		if err != nil {
			log.Error().Err(err).Msg("Failed to get arango search view.")
			return err
		}
		props, err := asv.Properties(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get properties for view.")
			return err
		}
		if err := backoff.Retry(func() error {
			// Create the view.
			if _, err := destinationDb.CreateArangoSearchView(ctx, v.Name(), &props); err != nil {
				log.Error().Err(err).Msg("Failed to create arango search view in destination db.")
				return err
			}
			return nil
		}, backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(backoffMaxTries)), ctx)); err != nil {
			c.Logger.Error().Err(err).Msg("Backoff eventually failed.")
			return err
		}
	}
	return nil
}

// copyIndexes
func (c *copier) copyIndexes(ctx context.Context, db driver.Database) error {
	return nil
}

// ensureDestinationDatabase ensures that a database exists at the destination.
func (c *copier) ensureDestinationDatabase(ctx context.Context, dbName string) error {
	c.Logger.Debug().Str("database-name", dbName).Msg("Ensuring database exists")
	if exists, err := c.destinationClient.DatabaseExists(ctx, dbName); err != nil {
		c.Logger.Warn().Err(err).Msg("Failed to get if database exists.")
		return err
	} else if exists {
		return nil
	}
	_, err := c.destinationClient.CreateDatabase(ctx, dbName, nil)
	return err
}

// ensureDestinationCollection ensures that a collection exists for a database on the destination and returns it.
func (c *copier) ensureDestinationCollection(ctx context.Context, db driver.Database, coll driver.Collection, props driver.CollectionProperties) (driver.Collection, error) {
	c.Logger.Debug().Str("collection-name", coll.Name()).Msg("Ensuring collection exists")
	exists, err := db.CollectionExists(ctx, coll.Name())
	if err != nil {
		c.Logger.Warn().Err(err).Msg("Failed to get if collection exists.")
		return nil, err
	}
	if !exists {
		return db.CreateCollection(ctx, coll.Name(), &driver.CreateCollectionOptions{
			JournalSize:       int(props.JournalSize),
			ReplicationFactor: props.ReplicationFactor,
			WriteConcern:      props.WriteConcern,
			WaitForSync:       props.WaitForSync,
			NumberOfShards:    props.NumberOfShards,
		})
	}
	return db.Collection(ctx, coll.Name())
}

// copyDatabase creates a database at the destination.
func (c *copier) copyDatabase(ctx context.Context, db driver.Database) error {
	if err := backoff.Retry(func() error {
		if err := c.ensureDestinationDatabase(ctx, db.Name()); err != nil {
			return err
		}
		return nil
	}, backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(backoffMaxTries)), ctx)); err != nil {
		c.Logger.Error().Err(err).Msg("Backoff eventually failed.")
		return err
	}
	return nil
}
