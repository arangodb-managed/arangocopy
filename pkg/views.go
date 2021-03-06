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
)

// copyViews copies all views from source database to destination database.
func (c *copier) copyViews(ctx context.Context, db driver.Database) error {
	log := c.Logger
	ctx = driver.WithIsRestore(ctx, true)
	var destinationDb driver.Database
	if err := c.backoffCall(ctx, func() error {
		// Get the destination database
		destDB, err := c.destinationClient.Database(ctx, db.Name())
		if err != nil {
			log.Error().Err(err).Msg("Failed to get destination database")
			return err
		}
		destinationDb = destDB
		return nil
	}); err != nil {
		return err
	}

	views, err := c.getViews(ctx, db)
	if err != nil {
		return err
	}

	for _, v := range views {
		log = log.With().Str("view", v.Name()).Str("db", db.Name()).Logger()

		// Check if view already exists
		var (
			exists bool
			props  driver.ArangoSearchViewProperties
		)
		if err := c.backoffCall(ctx, func() error {
			if ok, err := destinationDb.ViewExists(ctx, v.Name()); err != nil {
				log.Error().Err(err).Msg("Error checking if view exists.")
				return err
			} else {
				exists = ok
			}
			return nil
		}); err != nil {
			return err
		}

		if exists {
			continue
		}

		if err := c.backoffCall(ctx, func() error {
			asv, err := v.ArangoSearchView()
			if err != nil {
				log.Error().Err(err).Msg("Failed to get arango search view.")
				return err
			}
			propss, err := asv.Properties(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Failed to get properties for view.")
				return err
			}
			props = propss
			return nil
		}); err != nil {
			return err
		}

		if err := c.backoffCall(ctx, func() error {
			// Create the view.
			if _, err := destinationDb.CreateArangoSearchView(ctx, v.Name(), &props); err != nil {
				log.Error().Err(err).Msg("Failed to create arango search view in destination db.")
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// verifyViews verifies all views from source database to destination database.
func (c *copier) verifyViews(ctx context.Context, db driver.Database) error {
	log := c.Logger
	ctx = driver.WithIsRestore(ctx, true)
	views, err := c.getViews(ctx, db)
	if err != nil {
		return err
	}
	// Verify if views can be created at target location
	if err := c.Verifier.VerifyViews(ctx, views); err != nil {
		log.Error().Err(err).Msg("Verification failed to views.")
		return err
	}
	return nil
}

// getViews returns a filtered list of views for a given database.
func (c *copier) getViews(ctx context.Context, db driver.Database) ([]driver.View, error) {
	var views []driver.View
	if err := c.backoffCall(ctx, func() error {
		vs, err := db.Views(ctx)
		if err != nil {
			c.Logger.Error().Err(err).Msg("Failed to find all views.")
			return err
		}
		views = vs
		return nil
	}); err != nil {
		return nil, err
	}
	views = c.filterViews(views, db.Name())
	return views, nil
}
