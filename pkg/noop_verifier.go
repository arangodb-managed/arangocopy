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

type noopVerifier struct {
}

// NewNoopVerifier creates a no-op verifier.
func NewNoopVerifier() Verifier {
	return &noopVerifier{}
}

// VerifyDatabases takes a list of databases and verifies that each of them can be
// created at the destination.
func (n *noopVerifier) VerifyDatabases(ctx context.Context, source []driver.Database, destination driver.Client) error {
	return nil
}

// VerifyCollections takes a list of collections and verifies that each of them can be
// created at the destination.
func (n *noopVerifier) VerifyCollections(ctx context.Context, source []driver.Collection, destination driver.Database) error {
	return nil
}

// VerifyViews takes a list of views and verifies that each of them can be
// created at the destination.
func (n *noopVerifier) VerifyViews(ctx context.Context, source []driver.View, destination driver.Database) error {
	return nil
}

// VerifyIndexes takes a list of indexes and verifies that each of them can be
// created at the destination.
func (n *noopVerifier) VerifyIndexes(ctx context.Context, source []driver.Index, destination driver.Collection) error {
	return nil
}

// VerifyGraphs takes a list of graphs and verifies that each of them can be
// created at the destination.
func (n *noopVerifier) VerifyGraphs(ctx context.Context, source []driver.Graph, destination driver.Database) error {
	return nil
}
