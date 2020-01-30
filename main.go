package main

import (
	"context"
	"fmt"
	"os"

	driver "github.com/arangodb/go-driver"
	http "github.com/arangodb/go-driver/http"
	flag "github.com/spf13/pflag"
)

var (
	source string
	destination string
	dbname string
	includeSystem bool
	dropDestDatabase bool
)

func syncCollection(sdb driver.Database, scoll driver.Collection, sprops driver.CollectionProperties, ddb driver.Database) error {
	ctx := context.Background()
	dcoll, err := ddb.Collection(ctx, scoll.Name())
	if err != nil {
		dcoll, err = ddb.CreateCollection(ctx, scoll.Name(), &driver.CreateCollectionOptions{
        	JournalSize: int(sprops.JournalSize),
        	ReplicationFactor: sprops.ReplicationFactor,
        	WriteConcern: sprops.WriteConcern,
        	WaitForSync: sprops.WaitForSync,
        	NumberOfShards: sprops.NumberOfShards,
		})
		// FIXME: This needs more thought
		if err != nil {
			fmt.Fprintf(os.Stderr, "Did not find source database %s, error: %v\n", dbname, err)
			return err
		}
		fmt.Printf("Created collection %s\n", dcoll.Name())
	}
	bindVars := map[string]interface{}{
		"@c": scoll.Name(),
	}
	qctx := driver.WithQueryStream(ctx, true)
	cursor, err := sdb.Query(qctx, "FOR d IN @@c RETURN d", bindVars)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create cursor on collection %s, error: %v\n", scoll.Name(), err)
		return err
	}
	defer cursor.Close()

	wctx := driver.WithIsRestore(ctx, true)
	buffer := make([]interface{}, 4096)
	for {   // will be left by break
		var d interface{}
		meta, err := cursor.ReadDocument(qctx, &d)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not read document, error: %v\n", err)
			return err
		}
		fmt.Printf("Read document with key %s and rev %s\n", meta.Key, meta.Rev)
		buffer = append(buffer, d)
		if (!cursor.HasMore() && len(buffer) > 0) || len(buffer) >= 4096 {
			_, _, err = dcoll.CreateDocuments(wctx, buffer)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Could not write document batch, error: %v\n", err)
				return err
			}
			buffer = make([]interface{}, 4096)
		}
		if !cursor.HasMore() {
			break
		}
	}
	return nil
}

func syncDatabase(sclient driver.Client, dclient driver.Client, dbname string) error {
	drop := dropDestDatabase
	if dbname == "_system" {
		drop = false  // cannot drop the _system database
	}
	ctx := context.Background()
	sdb, err := sclient.Database(ctx, dbname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Did not find source database %s, error: %v\n", dbname, err)
		return err
	}
	exists, err := dclient.DatabaseExists(ctx, dbname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not determine existence of database %s in destination, error: %v\n", dbname, err)
		return err
	}
	var ddb driver.Database
	if exists && drop {
		ddb, err = dclient.Database(ctx, dbname)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Did not find destination database %s, error: %v\n", dbname, err)
			return err
		}
		err = ddb.Remove(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not drop database %s in destination, error: %v\n", dbname, err)
			return err
		}
	}
	if !drop {
		ddb, err = dclient.Database(ctx, dbname)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not get destination database _system, error: %v\n", err)
			return err
		}
	} else {
		ddb, err = dclient.CreateDatabase(ctx, dbname, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not create destination database %s, error: %v\n", dbname, err)
			return err
		}
	}

	scolls, err := sdb.Collections(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not list collections in source database %s, error: %v\n", dbname, err)
		return err
	}
	for _, c := range(scolls) {
		props, err := c.Properties(ctx)
		if err != nil {
			fmt.Printf("Could not get properties of collection %s in source, error: %v\n", c.Name(), err)
			return err
		}
		if includeSystem || !props.IsSystem {
			fmt.Printf("Doing collection %s ...\n", c.Name())
			err = syncCollection(sdb, c, props, ddb)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Could sync collection %s, error: %v\n", c.Name(), err)
				return err
			}
		}
	}
	return nil
}

func main() {
	flag.StringVar(&source, "source", "http://localhost:8529", "source server endpoint")
	flag.StringVar(&destination, "destination", "http://localhost:8530", "destination server endpoint")
	flag.StringVar(&dbname, "database", "_system", "database to sync")
	flag.BoolVar(&includeSystem, "includeSystem", false, "flag, if system collections are included")
	flag.BoolVar(&dropDestDatabase, "dropDestDatabase", true, "flag, if destination database is dropped before starting")
    flag.Parse()

	// Hello:
	fmt.Printf("Syncing database %s from endpoint %s to endpoint %s ...\n", dbname, source, destination)

	// Source connection and client:
	sconn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{source},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot connect to source endpoint %s, error: %v\n", source, err)
		os.Exit(2)
	}
	sclient, err := driver.NewClient(driver.ClientConfig{
		Connection: sconn,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot create source database client to endpoint %s, error: %v\n", source, err)
		os.Exit(3)
	}
	sversion, err := sclient.Version(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot get version of source server at %s, error: %v", source, err)
		os.Exit(4)
	}
	fmt.Printf("Version of source server: %v\n", sversion)

	// Destination connection and client:
	dconn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{destination},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot connect to destination endpoint %s, error: %v\n", destination, err)
		os.Exit(5)
	}
	dclient, err := driver.NewClient(driver.ClientConfig{
		Connection: dconn,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot create destination database client to endpoint %s, error: %v\n", destination, err)
		os.Exit(5)
	}
	dversion, err := dclient.Version(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot get version of destination server at %s, error: %v", destination, err)
		os.Exit(6)
	}
	fmt.Printf("Version of destination server: %v\n", dversion)

	// Now start the action:
	err = syncDatabase(sclient, dclient, dbname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error in syncing database: %v\n", err)
		os.Exit(5)
	}

	fmt.Print("Success, done.")
	os.Exit(0)
}

