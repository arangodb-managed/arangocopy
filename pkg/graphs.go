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

// copyGraphs copies all graphs from source database to destination database.
func (c *copier) copyGraphs(ctx context.Context, source, destination driver.Database) error {
	var graphs []driver.Graph
	if err := c.backoffCall(ctx, func() error {
		gs, err := source.Graphs(ctx)
		if err != nil {
			return err
		}
		graphs = gs
		return nil
	}); err != nil {
		return err
	}

	for _, g := range graphs {
		g.
		if _, err := destination.CreateGraph(ctx, g.Name(), &driver.CreateGraphOptions{
			OrphanVertexCollections: nil,
			EdgeDefinitions:         nil,
			IsSmart:                 false,
			SmartGraphAttribute:     "",
			NumberOfShards:          0,
			ReplicationFactor:       0,
			WriteConcern:            0,
		}); err != nil {
			return err
		}
		return nil
	}
	return nil
}
