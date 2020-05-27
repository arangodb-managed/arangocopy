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

import "github.com/arangodb/go-driver"

// filterDatabases takes a list of databases and filters it according to the set up
// included and excluded filters.
func (c *copier) filterDatabases(items []driver.Database) {
	for i := 0; i < len(items); i++ {
		if ok := isIncluded(items[i].Name(), c.databaseInclude, c.databaseExclude); !ok {
			items = append(items[:i], items[i+1:]...)
		}
	}
}

// filterCollections takes a list of collections and filters it according to the set up
// included and excluded filters.
func (c *copier) filterCollections(items []driver.Collection) {
	for i := 0; i < len(items); i++ {
		if ok := isIncluded(items[i].Name(), c.collectionInclude, c.collectionExclude); !ok {
			items = append(items[:i], items[i+1:]...)
		}
	}
}

// filterViews takes a list of views and filters it according to the set up
// included and excluded filters.
func (c *copier) filterViews(items []driver.View) {
	for i := 0; i < len(items); i++ {
		if ok := isIncluded(items[i].Name(), c.viewInclude, c.viewExclude); !ok {
			items = append(items[:i], items[i+1:]...)
		}
	}
}

// isIncluded will decide if an item with a given name should be included or excluded. This can be extended to do
// different kind of name matchings like partial or regex.
// If data is included in both, include and exclude, it will be excluded.
// data, include, exclude
func isIncluded(name string, include, exclude map[string]struct{}) (included bool) {
	if len(include) > 0 {
		_, ok := include[name]
		included = ok
	} else {
		included = true
	}
	if len(exclude) > 0 {
		if _, ok := exclude[name]; ok {
			included = false
		}
	}
	return
}
