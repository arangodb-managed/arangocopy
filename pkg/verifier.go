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

// Verifier defines the methods of a verification entity. This entity can verify all resources like
// collections, views, graphs etc, if they can be created at all at the destination. If, for example,
// a graph has a number of shards which is invalid at the destination, this verify will error out. Or
// copies like enterprise -> community edition will not work because of enterprise features.
type Verifier interface {
	// VerifyDatabases takes a list of databases and verifies that each of them can be
	// created at the destination.
	VerifyDatabases(ctx context.Context, source []driver.Database, destination driver.Client) error
	// VerifyCollections takes a list of collections and verifies that each of them can be
	// created at the destination.
	VerifyCollections(ctx context.Context, source []driver.Collection, destination driver.Database) error
	// VerifyViews takes a list of views and verifies that each of them can be
	// created at the destination.
	VerifyViews(ctx context.Context, source []driver.View, destination driver.Database) error
	// VerifyIndexes takes a list of indexes and verifies that each of them can be
	// created at the destination.
	VerifyIndexes(ctx context.Context, source []driver.Index, destination driver.Collection) error
	// VerifyGraphs takes a list of graphs and verifies that each of them can be
	// created at the destination.
	VerifyGraphs(ctx context.Context, source []driver.Graph, destination driver.Database) error
}
