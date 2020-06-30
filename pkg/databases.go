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

// copyDatabase creates a database at the destination.
func (c *copier) copyDatabase(ctx context.Context, db driver.Database) error {
	return c.backoffCall(ctx, func() error {
		if err := c.ensureDestinationDatabase(ctx, db); err != nil {
			return err
		}
		return nil
	})
}

// ensureDestinationDatabase ensures that a database exists at the destination.
func (c *copier) ensureDestinationDatabase(ctx context.Context, db driver.Database) error {
	c.Logger.Debug().Str("database-name", db.Name()).Msg("Ensuring database exists")
	if exists, err := c.destinationClient.DatabaseExists(ctx, db.Name()); err != nil {
		c.Logger.Warn().Err(err).Msg("Failed to get if database exists.")
		return err
	} else if exists {
		return nil
	}

	info, err := db.Info(ctx)
	if err != nil {
		c.Logger.Warn().Err(err).Msg("Failed to get database info.")
		return err
	}
	_, err = c.destinationClient.CreateDatabase(ctx, db.Name(), &driver.CreateDatabaseOptions{
		Options: driver.CreateDatabaseDefaultOptions{
			Sharding:          info.Sharding,
			ReplicationFactor: info.ReplicationFactor,
			WriteConcern:      info.WriteConcern,
		},
	})
	return err
}
