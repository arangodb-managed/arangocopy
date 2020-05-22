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

func (c *copier) filterDatabases(dbs []driver.Database) []driver.Database {
	names := make([]string, 0)
	for _, db := range dbs {
		names = append(names, db.Name())
	}
	filter := filterList(c.databaseInclude, c.databaseExclude, names)
	ret := make([]driver.Database, 0)
	for _, db := range dbs {
		if _, ok := filter[db.Name()]; ok {
			ret = append(ret, db)
		}
	}
	return ret
}

func (c *copier) filterCollections(items []driver.Collection) []driver.Collection {
	names := make([]string, 0)
	for _, item := range items {
		names = append(names, item.Name())
	}
	filter := filterList(c.collectionInclude, c.collectionExclude, names)
	ret := make([]driver.Collection, 0)
	for _, item := range items {
		if _, ok := filter[item.Name()]; ok {
			ret = append(ret, item)
		}
	}
	return ret
}

func (c *copier) filterViews(items []driver.View) []driver.View {
	names := make([]string, 0)
	for _, item := range items {
		names = append(names, item.Name())
	}
	filter := filterList(c.viewInclude, c.viewExclude, names)
	ret := make([]driver.View, 0)
	for _, item := range items {
		if _, ok := filter[item.Name()]; ok {
			ret = append(ret, item)
		}
	}
	return ret
}

// filterList constructs a list after filtering it with include and exclude data.
// if data is present in include and exclude it will be exluded.
func filterList(include, exclude map[string]struct{}, data []string) map[string]struct{} {
	filter := make(map[string]struct{})
	if len(include) > 0 {
		for i := 0; i < len(data); i++ {
			if _, ok := include[data[i]]; ok {
				filter[data[i]] = struct{}{}
			}
		}
	} else {
		for _, d := range data {
			filter[d] = struct{}{}
		}
	}
	if len(exclude) > 0 {
		for i := 0; i < len(data); i++ {
			if _, ok := exclude[data[i]]; ok {
				delete(filter, data[i])
			}
		}
	}
	return filter
}
