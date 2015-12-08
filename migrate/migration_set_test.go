package migrate_test

import (
	"encoding/json"

	"github.com/belogik/goes"
	. "github.com/pib/esit/migrate"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func mustJSON(s string) map[string]interface{} {
	m := map[string]interface{}{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		panic(err.Error())
	}
	return m
}

var _ = Describe("MigrationSet", func() {
	settings := []map[string]interface{}{
		mustJSON(`{
			"mappings": {
				"things": {
					"properties": {
						"name": {"type": "string"}
					}
				}
			}
		}`),
		mustJSON(`{
			"mappings": {
				"things": {
					"properties": {
						"name": {"type": "string", "index": "not_analyzed"}
					}
				}
			}
		}`),
	}
	migrations := []Migration{
		Migration{FromIndex: "", ToIndex: "zero", Settings: settings[0]},
		Migration{FromIndex: "zero", ToIndex: "one", Settings: settings[1]},
	}

	Describe("Run", func() {
		var err error
		var conn *mockConnection
		var migrationSet *MigrationSet

		BeforeEach(func() {
			err = nil
			conn = &mockConnection{}
			migrationSet = &MigrationSet{}
		})

		JustBeforeEach(func() {
			err = migrationSet.Run(conn)
		})

		Context("When there is one migration and no existing indices", func() {
			BeforeEach(func() {
				migrationSet.Migrations = migrations[0:1]
				conn.indicesExistCalls.returns = [][]interface{}{{false, nil}}
				conn.createIndexCalls.returns = [][]interface{}{{&goes.Response{}, nil}}
			})

			It("should only check if the destination index exists", func() {
				Expect(conn.indicesExistCalls.calls).To(Equal([][]interface{}{{[]string{"zero"}}}))
			})

			It("should create the initial index", func() {
				Expect(conn.createIndexCalls.calls).To(Equal([][]interface{}{{"zero", settings[0]}}))
			})
		})

		Context("When there are two migrations and no existing indices", func() {
			BeforeEach(func() {
				migrationSet.Migrations = migrations[0:2]
				conn.indicesExistCalls.returns = [][]interface{}{{false, nil}, {false, nil}}
				conn.createIndexCalls.returns = [][]interface{}{{&goes.Response{}, nil}}
			})

			It("should check if both indices exist", func() {
				Expect(conn.indicesExistCalls.calls).To(Equal([][]interface{}{
					{[]string{"zero"}},
					{[]string{"one"}},
				}))
			})

			It("should create the second index", func() {
				Expect(conn.createIndexCalls.calls).To(Equal([][]interface{}{{"one", settings[1]}}))
			})
		})

	})
})
