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
	"os"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"

	"github.com/arangodb-managed/arangocopy/pkg"
)

var (
	// RootCmd is the root (and only) command of this service
	RootCmd = &cobra.Command{
		Use:   "arangocopy",
		Short: "ArangoDB Copy",
		Long:  "ArangoDB Copy. Copy data across arangodb instances in an easy, user-friendly and resilient way.",
		Run:   run,
	}
	// CLILog is an instance of a zerolog logger.
	CLILog = zerolog.New(zerolog.ConsoleWriter{
		Out: os.Stderr,
	}).With().Timestamp().Logger()
	// RootArgs are the arguments for the root command.
	RootArgs struct {
		source                 pkg.Connection
		destination            pkg.Connection
		includedDatabases      []string
		excludedDatabases      []string
		includedCollections    []string
		excludedCollections    []string
		includedViews          []string
		excludedViews          []string
		includedGraphs         []string
		excludedGraphs         []string
		force                  bool
		maxParallelCollections int
		batchSize              int
		maxRetries             int
	}
)

func init() {
	f := RootCmd.PersistentFlags()
	f.StringVarP(&RootArgs.source.Address, "source-address", "s", "", "Source database address to copy data from.")
	f.StringVar(&RootArgs.source.Username, "source-username", "", "Source database username if required.")
	f.StringVar(&RootArgs.source.Password, "source-password", "", "Source database password if required.")
	f.StringVarP(&RootArgs.destination.Address, "destination-address", "d", "", "Destination database address to copy data to.")
	f.StringVar(&RootArgs.destination.Username, "destination-username", "", "Destination database username if required.")
	f.StringVar(&RootArgs.destination.Password, "destination-password", "", "Destination database password if required.")
	f.IntVarP(&RootArgs.maxParallelCollections, "maximum-parallel-collections", "m", 10, "Maximum number of collections being copied in parallel.")
	f.StringSliceVar(&RootArgs.includedDatabases, "included-database", []string{}, "A list of database names which should be included. If provided, only these databases will be copied.")
	f.StringSliceVar(&RootArgs.excludedDatabases, "exluded-database", []string{}, "A list of database names which should be excluded. Exclusion takes priority over inclusion.")
	f.StringSliceVar(&RootArgs.includedCollections, "included-collection", []string{}, "A list of collection names which should be included. If provided, only these collections will be copied.")
	f.StringSliceVar(&RootArgs.excludedCollections, "excluded-collection", []string{}, "A list of collections names which should be excluded. Exclusion takes priority over inclusion.")
	f.StringSliceVar(&RootArgs.includedViews, "included-view", []string{}, "A list of view names which should be included. If provided, only these views will be copied.")
	f.StringSliceVar(&RootArgs.excludedViews, "excluded-view", []string{}, "A list of view names which should be excluded. Exclusion takes priority over inclusion.")
	f.StringSliceVar(&RootArgs.includedGraphs, "included-graph", []string{}, "A list of graph names which should be included. If provided, only these graphs will be copied.")
	f.StringSliceVar(&RootArgs.excludedGraphs, "excluded-graph", []string{}, "A list of graph names which should be excluded. Exclusion takes priority over inclusion.")
	f.BoolVarP(&RootArgs.force, "force", "f", false, "Force the copy automatically overwriting everything at destination.")
	f.IntVarP(&RootArgs.batchSize, "batch-size", "b", 4096, "The number of documents to write at once.")
	f.IntVarP(&RootArgs.maxRetries, "max-retries", "r", 9, "The number of maximum retries attempts. Increasing this number will also increase the exponential fallback timer.")
}

// run runs the copy operation.
func run(cmd *cobra.Command, args []string) {
	// Validate arguments
	log := CLILog
	_, argsUsed := ReqOption("source-address", RootArgs.source.Address, args, 0)
	_, argsUsed = ReqOption("destination-address", RootArgs.destination.Address, args, 1)
	MustCheckNumberOfArgs(args, argsUsed)

	// Create copier
	copier, err := pkg.NewCopier(pkg.Config{
		Force:                      RootArgs.force,
		Source:                     RootArgs.source,
		BatchSize:                  RootArgs.batchSize,
		MaxRetries:                 RootArgs.maxRetries,
		Destination:                RootArgs.destination,
		IncludedViews:              RootArgs.includedViews,
		ExcludedViews:              RootArgs.excludedViews,
		IncludedGraphs:             RootArgs.includedGraphs,
		ExcludedGraphs:             RootArgs.excludedGraphs,
		IncludedDatabases:          RootArgs.includedDatabases,
		ExcludedDatabases:          RootArgs.excludedDatabases,
		IncludedCollections:        RootArgs.includedCollections,
		ExcludedCollections:        RootArgs.excludedCollections,
		MaximumParallelCollections: RootArgs.maxParallelCollections,
	}, pkg.Dependencies{
		Logger: CLILog,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start copy operation.")
	}

	// Start copy operatio
	if err := copier.Copy(); err != nil {
		log.Fatal().Err(err).Msg("Failed to copy. Please try again after the issue is resolved.")
	}
	log.Info().Msg("Success!")
}

// ReqOption returns given value if not empty.
// Fails with clear error message when not set.
// Returns: option-value, number-of-args-used(0|argIndex+1)
func ReqOption(key, value string, args []string, argIndex int) (string, int) {
	if value != "" {
		return value, 0
	}
	if len(args) > argIndex {
		return args[argIndex], argIndex + 1
	}
	CLILog.Fatal().Msgf("--%s missing", key)
	return "", 0
}

// MustCheckNumberOfArgs compares the number of arguments with the expected
// number of arguments.
// If there is a difference a fatal error is raised.
func MustCheckNumberOfArgs(args []string, expectedNumberOfArgs int) {
	if len(args) > expectedNumberOfArgs {
		CLILog.Fatal().Msg("Too many arguments")
	}
	if len(args) < expectedNumberOfArgs {
		CLILog.Fatal().Msg("Too few arguments")
	}
}
