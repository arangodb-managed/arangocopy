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
	flag "github.com/spf13/pflag"
)

var (
	// RootCmd is the root (and only) command of this service
	RootCmd = &cobra.Command{
		Use:   "arangocopy",
		Short: "ArangoDB Copy",
		Long:  "ArangoDB Copy. The copy tool of the ages.",
		Run:   ShowUsage,
	}

	CLILog = zerolog.New(zerolog.ConsoleWriter{
		Out: os.Stderr,
	}).With().Timestamp().Logger()
	RootArgs struct {
	}
)

// ShowUsage shows usage of the given command on stdout.
func ShowUsage(cmd *cobra.Command, args []string) {
	cmd.Usage()
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

// OptOption returns given value if not empty.
// Returns: option-value, number-of-args-used(0|argIndex+1)
func OptOption(key, value string, args []string, argIndex int) (string, int) {
	if value != "" {
		return value, 0
	}
	if len(args) > argIndex {
		return args[argIndex], argIndex + 1
	}
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

// InitCommand adds the given command to the given parent and called the flag initialization
// function.
func InitCommand(parent, cmd *cobra.Command, flagInit func(c *cobra.Command, f *flag.FlagSet)) *cobra.Command {
	if parent != nil {
		parent.AddCommand(cmd)
	}
	flagInit(cmd, cmd.Flags())
	return cmd
}
