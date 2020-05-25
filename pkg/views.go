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

	"github.com/arangodb/go-driver"
	"github.com/cenkalti/backoff"
)

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
