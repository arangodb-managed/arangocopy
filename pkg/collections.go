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
	"errors"

	"github.com/arangodb/go-driver"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// copyCollections copies all collections for a database.
func (c *copier) copyCollections(ctx context.Context, db driver.Database) error {
	log := c.Logger
	log.Info().Msg("Beginning to copy over collection data.")
	var (
		sourceDB      driver.Database
		collections   []driver.Collection
		destinationDB driver.Database
	)
	if err := c.backoffCall(ctx, func() error {
		sdb, err := c.sourceClient.Database(ctx, db.Name())
		if err != nil {
			return err
		}
		sourceDB = sdb
		return nil
	}); err != nil {
		return err
	}
	if err := c.backoffCall(ctx, func() error {
		colls, err := sourceDB.Collections(ctx)
		if err != nil {
			c.Logger.Error().Err(err).Msg("Failed to list collections for source database.")
			return err
		}
		collections = colls
		return nil
	}); err != nil {
		return err
	}
	if err := c.backoffCall(ctx, func() error {
		ddb, err := c.destinationClient.Database(ctx, sourceDB.Name())
		if err != nil {
			c.Logger.Error().Err(err).Msg("Failed to get destination database.")
			return nil
		}
		destinationDB = ddb
		return nil
	}); err != nil {
		return err
	}

	collections = c.filterCollections(collections)

	// Verify if collections can be created at target location
	if err := c.Verifier.VerifyCollections(ctx, collections, destinationDB); err != nil {
		log.Error().Err(err).Msg("Verifier failed for collections.")
		return err
	}

	readCtx := driver.WithQueryStream(ctx, true)
	readCtx = driver.WithQueryBatchSize(readCtx, c.BatchSize)
	restoreCtx := driver.WithIsRestore(ctx, true)
	var g errgroup.Group
	sem := semaphore.NewWeighted(int64(c.MaximumParallelCollections))
	if c.Dependencies.Spinner != nil {
		c.Dependencies.Spinner.Start()
	}
	for _, sourceColl := range collections {
		sourceColl := sourceColl
		g.Go(func() error {
			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}
			defer sem.Release(1)
			var props driver.CollectionProperties
			if err := c.backoffCall(ctx, func() error {
				sourceProps, err := sourceColl.Properties(ctx)
				if err != nil {
					c.Logger.Error().Err(err).Msg("Failed to get properties.")
					return err
				}
				props = sourceProps
				return nil
			}); err != nil {
				return err
			}
			if props.IsSystem {
				// skip system collections
				return nil
			}
			var destinationColl driver.Collection
			if err := c.backoffCall(ctx, func() error {
				dColl, err := c.ensureDestinationCollection(ctx, destinationDB, sourceColl, props)
				if err != nil {
					c.Logger.Error().Err(err).Msg("Failed to ensure destination collection.")
					return err
				}
				destinationColl = dColl
				return nil
			}); err != nil {
				return err
			}

			// Copy over all indexes for this collection.
			if err := c.copyIndexes(restoreCtx, sourceColl, destinationColl); err != nil {
				c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Msg("Failed to copy all indexes.")
				return err
			}

			bindVars := map[string]interface{}{
				"@c": sourceColl.Name(),
			}
			var cursor driver.Cursor
			if err := c.backoffCall(ctx, func() error {
				cr, err := sourceDB.Query(readCtx, "FOR d IN @@c RETURN d", bindVars)
				if err != nil {
					c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Msg("Failed to query source database for collection.")
					return err
				}
				cursor = cr
				return nil
			}); err != nil {
				return err
			}
			defer cursor.Close()
			batch := make([]interface{}, 0, c.BatchSize)
			for {
				var (
					d           interface{}
					noMoreError bool
				)
				if err := c.backoffCall(ctx, func() error {
					if _, err := cursor.ReadDocument(readCtx, &d); driver.IsNoMoreDocuments(err) {
						noMoreError = true
						return nil
					} else if err != nil {
						c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Msg("Read documents failed.")
						return err
					}
					batch = append(batch, d)
					return nil
				}); err != nil {
					return err
				}

				if (noMoreError && len(batch) > 0) || len(batch) >= c.BatchSize {
					if err := c.backoffCall(ctx, func() error {
						if _, _, err := destinationColl.CreateDocuments(restoreCtx, batch); err != nil {
							c.Logger.Error().Err(err).Str("collection", sourceColl.Name()).Interface("document", d).Msg("Creating a document failed.")
							return err
						}
						batch = make([]interface{}, 0, c.BatchSize)
						return nil
					}); err != nil {
						return err
					}
				}

				if noMoreError {
					break
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Error().Err(err).Msg("One of the workers failed to copy data.")
		return err
	}
	if c.Dependencies.Spinner != nil {
		c.Dependencies.Spinner.Stop()
	}
	log.Debug().Str("source-database", sourceDB.Name()).Msg("Done copying database data.")
	return nil
}

// copyIndexes copies all indexes for a collection to destination collection.
func (c *copier) copyIndexes(ctx context.Context, sourceColl driver.Collection, destinationColl driver.Collection) error {
	var indexes []driver.Index
	if err := c.backoffCall(ctx, func() error {
		idxs, err := sourceColl.Indexes(ctx)
		if err != nil {
			return err
		}
		indexes = idxs
		return nil
	}); err != nil {
		return err
	}

	// Check if index can be created at target location
	if err := c.Verifier.VerifyIndexes(ctx, indexes, destinationColl); err != nil {
		log.Error().Err(err).Msg("Verification failed for indexes.")
		return err
	}

	for _, index := range indexes {
		if err := c.backoffCall(ctx, func() error {
			switch index.Type() {
			case driver.TTLIndex:
				var field string
				if len(index.Fields()) > 0 {
					field = index.Fields()[0]
				}
				if _, _, err := destinationColl.EnsureTTLIndex(ctx, field, index.ExpireAfter(), &driver.EnsureTTLIndexOptions{
					Name: index.UserName(),
				}); err != nil {
					return err
				}
			case driver.PersistentIndex:
				if _, _, err := destinationColl.EnsurePersistentIndex(ctx, index.Fields(), &driver.EnsurePersistentIndexOptions{
					Name:   index.UserName(),
					Unique: index.Unique(),
					Sparse: index.Sparse(),
				}); err != nil {
					return err
				}
			case driver.SkipListIndex:
				if _, _, err := destinationColl.EnsureSkipListIndex(ctx, index.Fields(), &driver.EnsureSkipListIndexOptions{
					Name:          index.UserName(),
					Unique:        index.Unique(),
					Sparse:        index.Sparse(),
					NoDeduplicate: !index.Deduplicate(),
				}); err != nil {
					return err
				}
			case driver.HashIndex:
				if _, _, err := destinationColl.EnsureHashIndex(ctx, index.Fields(), &driver.EnsureHashIndexOptions{
					Name:          index.UserName(),
					Unique:        index.Unique(),
					Sparse:        index.Sparse(),
					NoDeduplicate: !index.Deduplicate(),
				}); err != nil {
					return err
				}
			case driver.FullTextIndex:
				if _, _, err := destinationColl.EnsureFullTextIndex(ctx, index.Fields(), &driver.EnsureFullTextIndexOptions{
					Name:      index.UserName(),
					MinLength: index.MinLength(),
				}); err != nil {
					return err
				}
			case driver.GeoIndex:
				if _, _, err := destinationColl.EnsureGeoIndex(ctx, index.Fields(), &driver.EnsureGeoIndexOptions{
					Name:    index.UserName(),
					GeoJSON: index.GeoJSON(),
				}); err != nil {
					return err
				}
			case driver.EdgeIndex:
				// These are automatically created.
			case driver.PrimaryIndex:
				// These are automatically created.
			default:
				return errors.New("unknown index type " + string(index.Type()))
			}
			return nil
		}); err != nil {
			return err
		}
	}
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
		options := &driver.CreateCollectionOptions{
			JournalSize:       int(props.JournalSize),
			ReplicationFactor: props.ReplicationFactor,
			WriteConcern:      props.WriteConcern,
			WaitForSync:       props.WaitForSync,
			DoCompact:         &props.DoCompact,
			CacheEnabled:      &props.CacheEnabled,
			ShardKeys:         props.ShardKeys,
			NumberOfShards:    props.NumberOfShards,
			IsSystem:          false,
			Type:              props.Type,
			KeyOptions: &driver.CollectionKeyOptions{
				AllowUserKeys: props.KeyOptions.AllowUserKeys,
				Type:          props.KeyOptions.Type,
			},
			DistributeShardsLike: props.DistributeShardsLike,
			IsSmart:              false,
			ShardingStrategy:     props.ShardingStrategy,
		}
		if props.SmartJoinAttribute != "" {
			options.IsSmart = true
			options.SmartJoinAttribute = props.SmartJoinAttribute
		}
		return db.CreateCollection(ctx, coll.Name(), options)
	}
	return db.Collection(ctx, coll.Name())
}
