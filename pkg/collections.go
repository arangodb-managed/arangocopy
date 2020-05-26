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
	"time"

	"github.com/arangodb/go-driver"
	"github.com/briandowns/spinner"
	"github.com/cenkalti/backoff"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

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
	destinationDB, err := c.destinationClient.Database(ctx, sourceDB.Name())
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to get destination database.")
		return nil
	}
	readCtx := driver.WithQueryStream(ctx, true)
	readCtx = driver.WithQueryBatchSize(readCtx, c.BatchSize)
	restoreCtx := driver.WithIsRestore(ctx, true)
	var g errgroup.Group
	sem := semaphore.NewWeighted(int64(c.Parallel))
	s := spinner.New(spinner.CharSets[34], 100*time.Millisecond)
	s.Start()
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
			cursor.Close()

			// Copy over all indexes for this collection.
			if err := c.copyIndexes(restoreCtx, sourceColl, destinationColl); err != nil {
				c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Msg("Failed to copy all indexes.")
				return err
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Error().Err(err).Msg("One of the workers failed to copy data.")
		return err
	}
	s.Stop()
	log.Debug().Str("source-database", sourceDB.Name()).Msg("Done copying database data.")
	return nil
}

// copyIndexes copies all indexes for a collection to destination collection.
func (c *copier) copyIndexes(ctx context.Context, sourceColl driver.Collection, destinationColl driver.Collection) error {
	return nil
}

// ensureDestinationCollection ensures that a collection exists for a database on the destination and returns it.
func (c *copier) ensureDestinationCollection(ctx context.Context, db driver.Database, coll driver.Collection, props driver.CollectionProperties) (driver.Collection, error) {
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
