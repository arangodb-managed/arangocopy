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

package cmd

import (
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"

	"github.com/arangodb-managed/arangocopy/pkg"
)

func init() {
	InitCommand(
		RootCmd,
		&cobra.Command{
			Use:   "copy",
			Short: "Copy data from source to destination.",
		},
		func(c *cobra.Command, f *flag.FlagSet) {
			cargs := &struct {
				sourceAddress          string
				sourceUsername         string
				sourcePassword         string
				destinationAddress     string
				destinationUsername    string
				destinationPassword    string
				includedDatabases      []string
				excludedDatabases      []string
				includedCollections    []string
				excludedCollections    []string
				includedViews          []string
				excludedViews          []string
				force                  bool
				maxParallelCollections int
				timeout                float64
				batchSize              int
			}{}
			f.StringVarP(&cargs.sourceAddress, "source-address", "s", "", "Source database address to copy data from.")
			f.StringVar(&cargs.sourceUsername, "source-username", "", "Source database username if required.")
			f.StringVar(&cargs.sourcePassword, "source-password", "", "Source database password if required.")
			f.StringVarP(&cargs.destinationAddress, "destination-address", "d", "", "Destination database address to copy data to.")
			f.StringVar(&cargs.destinationUsername, "destination-username", "", "Destination database username if required.")
			f.StringVar(&cargs.destinationPassword, "destination-password", "", "Destination database password if required.")
			f.IntVarP(&cargs.maxParallelCollections, "maximum-parallel-collections", "m", 5, "Maximum number of collections being read out of in parallel.")
			f.StringSliceVar(&cargs.includedDatabases, "included-databases", []string{}, "A list of database names which should be included. If provided, only these databases will be copied.")
			f.StringSliceVar(&cargs.includedCollections, "included-collections", []string{}, "A list of collection names which should be included. If provided, only these collections will be copied.")
			f.StringSliceVar(&cargs.includedViews, "included-views", []string{}, "A list of view names which should be included. If provided, only these views will be copied.")
			f.StringSliceVar(&cargs.excludedDatabases, "exluded-databases", []string{}, "A list of database names which should be excluded. Exclusion takes priority over inclusion.")
			f.StringSliceVar(&cargs.excludedCollections, "excluded-collections", []string{}, "A list of collections names which should be excluded. Exclusion takes priority over inclusion.")
			f.StringSliceVar(&cargs.excludedViews, "excluded-views", []string{}, "A list of view names which should be excluded. Exclusion takes priority over inclusion.")
			f.BoolVarP(&cargs.force, "force", "f", false, "Force the copy automatically overwriting everything at destination.")
			f.Float64VarP(&cargs.timeout, "timeout", "t", 30, "Timeout in seconds for all queries. Value of 0 means no timeout. Allows for fractions.")
			f.IntVarP(&cargs.batchSize, "batch-size", "b", 4096, "The number of documents to write at once.")

			c.Run = func(c *cobra.Command, args []string) {
				// Validate arguments
				log := CLILog
				sourceAddress, argsUsed := ReqOption("source-address", cargs.sourceAddress, args, 0)
				destinationAddress, argsUsed := ReqOption("destination-address", cargs.destinationAddress, args, 1)
				MustCheckNumberOfArgs(args, argsUsed)
				copier, err := pkg.NewCopier(pkg.Config{
					SourceAddress:       sourceAddress,
					SourceUsername:      cargs.sourceUsername,
					SourcePassword:      cargs.sourcePassword,
					DestinationAddress:  destinationAddress,
					DestinationUsername: cargs.destinationUsername,
					DestinationPassword: cargs.destinationPassword,
					IncludedDatabases:   cargs.includedDatabases,
					IncludedCollections: cargs.includedCollections,
					IncludedViews:       cargs.includedViews,
					ExcludedDatabases:   cargs.excludedDatabases,
					ExcludedCollections: cargs.excludedCollections,
					ExcludedViews:       cargs.excludedViews,
					Force:               cargs.force,
					Parallel:            cargs.maxParallelCollections,
					Timeout:             cargs.timeout,
					BatchSize:           cargs.batchSize,
				}, pkg.Dependencies{
					Logger: CLILog,
				})
				if err != nil {
					log.Fatal().Err(err).Msg("Failed to start copy operation.")
				}
				if err := copier.Copy(); err != nil {
					log.Fatal().Err(err).Msg("Failed to copy. Please try again after issue is resolved.")
				}
				log.Info().Msg("Success!")
			}
		},
	)
}
