# Arangocopy

![ArangoDB Oasis](https://cloud.arangodb.com/assets/logos/arangodb-oasis-logo-whitebg-right.png)

Commandline utility for coping data across ArangoDB instances.

This utility is being used to copy data across ArangoDB databases in a safe way. The tool is resilient to network failures
and allows for continuing an operation where it left off.

## Warning!

This tool will **OVERWRITE** whatever destination has. 

## Maintainers

This utility is maintained by the team at [ArangoDB](https://www.arangodb.com/).

## Installation

Downloading the [latest released binaries](https://github.com/arangodb-managed/arangocopy/releases),
extract the zip archive and install the binary for your platform in your preferred location.

Or to build from source, run:

```bash
git clone https://github.com/arangodb-managed/arangocopy.git
make
```

## Usage

```bash
arangocopy [command...]
```

A list of commands and options can be shown using:

```bash
arangocopy -h
```
