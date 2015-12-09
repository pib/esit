package main

import (
	"fmt"
	"strings"

	"github.com/belogik/goes"
	"github.com/pib/esit/migrate"
)

type m map[string]interface{} // For convenience below.

var migrations = []migrate.Migration{
	{
		ToIndex: "people-00",
		Settings: m{
			"mappings": m{
				"person": m{
					"properties": m{
						"name": m{"type": "string"},
					},
				},
			},
		},
	},
	{
		FromIndex: "people-00",
		ToIndex:   "people-01",
		Settings: m{
			"mappings": m{
				"person": m{
					"properties": m{
						"given_name":  m{"type": "string"},
						"family_name": m{"type": "string"},
					},
				},
			},
		},
		TypeTransformers: map[string]func(*goes.Document) []*goes.Document{
			"person": func(doc *goes.Document) []*goes.Document {
				nameParts := strings.SplitN(doc.Fields.(map[string]interface{})["name"].(string), " ", 2)
				doc.Fields = m{
					"given_name":  nameParts[0],
					"family_name": strings.Join(nameParts[1:], " "),
				}
				return []*goes.Document{doc}
			},
		},
	},
}

var migrationSets = []migrate.MigrationSet{
	{
		IndexAlias: "people",
		Migrations: migrations[0:1],
	},
	{
		IndexAlias: "people",
		Migrations: migrations[0:2],
	},
}

func main() {
	conn := migrate.NewESConnection("localhost", "9200")

	fmt.Printf("Initial migration: (error: %#v)\n", migrationSets[0].Run(conn))
	conn.Index(goes.Document{Index: "people-00", Type: "person", Id: "bob", Fields: m{"name": "Bob Boberson"}}, nil)
	conn.Index(goes.Document{Index: "people-00", Type: "person", Id: "fred", Fields: m{"name": "Fred Frederson"}}, nil)
	conn.RefreshIndex("people-00")
	printPeople(conn)

	fmt.Printf("Second migration: (error: %#v)\n", migrationSets[1].Run(conn))
	conn.RefreshIndex("people-01")
	printPeople(conn)
}

func printPeople(c migrate.Connection) {
	c.ForEach(m{"query": m{"match_all": m{}}}, []string{"people"}, nil, func(doc *goes.Document) error {
		fmt.Printf("%#v\n", doc)
		return nil
	})
}
