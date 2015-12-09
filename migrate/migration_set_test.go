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
		mustJSON(`{
			"mappings": {
				"things": {
					"properties": {
						"name": {"type": "string", "index": "not_analyzed"},
						"description": {"type": "string"}
					}
				}
			}
		}`),
	}
	migrations := []Migration{
		Migration{FromIndex: "", ToIndex: "zero", Settings: settings[0]},
		Migration{FromIndex: "zero", ToIndex: "one", Settings: settings[1],
			TypeTransformers: map[string]func(*goes.Document) []*goes.Document{
				"things": func(doc *goes.Document) []*goes.Document {
					name := doc.Fields.(map[string]interface{})["name"].(string)
					return []*goes.Document{
						doc,
						&goes.Document{Index: doc.Index, Type: "things", Fields: map[string]interface{}{"name": name + " Copy"}},
					}
				},
			}},
		Migration{FromIndex: "one", ToIndex: "two", Settings: settings[2],
			TypeTransformers: map[string]func(*goes.Document) []*goes.Document{
				"things": func(doc *goes.Document) []*goes.Document {
					name := doc.Fields.(map[string]interface{})["name"].(string)
					doc.Fields.(map[string]interface{})["description"] = "About " + name
					return []*goes.Document{doc}
				},
			}},
	}

	Describe("Run", func() {
		var err error
		var conn *mockConnection
		var migrationSet *MigrationSet

		BeforeEach(func() {
			err = nil
			conn = &mockConnection{}
			migrationSet = &MigrationSet{IndexAlias: "test"}
		})

		JustBeforeEach(func() {
			err = migrationSet.Run(conn)
		})

		Context("When there is one migration and no existing indices", func() {
			BeforeEach(func() {
				migrationSet.Migrations = migrations[0:1]
				conn.customIndicesExist = func(indices []string) (bool, error) {
					return false, nil
				}
				conn.createIndexCalls.returns = [][]interface{}{{&goes.Response{}, nil}}
			})

			It("should check if the destination index exists", func() {
				Expect(conn.indicesExistCalls.calls).To(ContainElement([]interface{}{[]string{"zero"}}))
			})

			It("should create the initial index", func() {
				Expect(conn.createIndexCalls.calls).To(Equal([][]interface{}{{"zero", settings[0]}}))
			})

			It("should point the alias to the initial index", func() {
				Expect(conn.addAliasCalls.calls).To(Equal([][]interface{}{{"test", []string{"zero"}}}))
			})

			It("should not attempt to remove an alias", func() {
				Expect(conn.removeAliasCalls.calls).To(BeEmpty())
			})
		})

		Context("When there are two migrations and no existing indices", func() {
			BeforeEach(func() {
				migrationSet.Migrations = migrations[0:2]
				conn.customIndicesExist = func(indices []string) (bool, error) {
					return false, nil
				}
				conn.createIndexCalls.returns = [][]interface{}{{&goes.Response{}, nil}}
			})

			It("should check if both indices exist", func() {
				Expect(conn.indicesExistCalls.calls).To(ContainElement([]interface{}{[]string{"zero"}}))
				Expect(conn.indicesExistCalls.calls).To(ContainElement([]interface{}{[]string{"one"}}))
			})

			It("should create the second index", func() {
				Expect(conn.createIndexCalls.calls).To(Equal([][]interface{}{{"one", settings[1]}}))
			})

			It("should point the alias to the second index", func() {
				Expect(conn.addAliasCalls.calls).To(Equal([][]interface{}{{"test", []string{"one"}}}))
			})

			It("should not attempt to remove an alias", func() {
				Expect(conn.removeAliasCalls.calls).To(BeEmpty())
			})
		})

		Context("When there are three migrations and no existing indices", func() {
			BeforeEach(func() {
				migrationSet.Migrations = migrations[0:3]
				conn.customIndicesExist = func(indices []string) (bool, error) {
					return false, nil
				}
				conn.createIndexCalls.returns = [][]interface{}{{&goes.Response{}, nil}}
			})

			It("should check if all three indices exist", func() {
				Expect(conn.indicesExistCalls.calls).To(ContainElement([]interface{}{[]string{"zero"}}))
				Expect(conn.indicesExistCalls.calls).To(ContainElement([]interface{}{[]string{"one"}}))
				Expect(conn.indicesExistCalls.calls).To(ContainElement([]interface{}{[]string{"two"}}))
			})

			It("should create the third index", func() {
				Expect(conn.createIndexCalls.calls).To(Equal([][]interface{}{{"two", settings[2]}}))
			})

			It("should point the alias to the third index", func() {
				Expect(conn.addAliasCalls.calls).To(Equal([][]interface{}{{"test", []string{"two"}}}))
			})

			It("should not attempt to remove an alias", func() {
				Expect(conn.removeAliasCalls.calls).To(BeEmpty())
			})
		})

		Context("When the first of three indices exists", func() {
			BeforeEach(func() {
				migrationSet.Migrations = migrations[0:3]
				conn.customIndicesExist = func(indices []string) (bool, error) {
					if indices[0] == "zero" {
						return true, nil
					}
					return false, nil
				}
				conn.createIndexCalls.returns = [][]interface{}{{&goes.Response{}, nil}}
				conn.forEachCalls.returns = [][]interface{}{
					{
						[]*goes.Document{
							{
								Index:  "zero",
								Type:   "things",
								Id:     "1",
								Fields: map[string]interface{}{"name": "Thing One"},
							},
							{
								Index:  "zero",
								Type:   "things",
								Id:     "2",
								Fields: map[string]interface{}{"name": "Thing Two"},
							},
						},
						nil,
					},
				}
				conn.indexCalls.returns = [][]interface{}{{&goes.Response{}, nil}, {&goes.Response{}, nil}, {&goes.Response{}, nil}, {&goes.Response{}, nil}}
			})

			It("should create the third index", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(conn.createIndexCalls.calls).To(Equal([][]interface{}{{"two", settings[2]}}))
			})

			It("should copy the docs to the third index", func() {
				Expect(conn.indexCalls.calls[0][0]).To(Equal(
					goes.Document{Index: "two", Type: "things", Id: "1", BulkCommand: "index", Fields: map[string]interface{}{
						"name":        "Thing One",
						"description": "About Thing One",
					}}))
				Expect(conn.indexCalls.calls[1][0]).To(Equal(
					goes.Document{Index: "two", Type: "things", BulkCommand: "index", Fields: map[string]interface{}{
						"name":        "Thing One Copy",
						"description": "About Thing One Copy",
					}}))
				Expect(conn.indexCalls.calls[2][0]).To(Equal(
					goes.Document{Index: "two", Type: "things", Id: "2", BulkCommand: "index", Fields: map[string]interface{}{
						"name":        "Thing Two",
						"description": "About Thing Two",
					}}))
				Expect(conn.indexCalls.calls[3][0]).To(Equal(
					goes.Document{Index: "two", Type: "things", BulkCommand: "index", Fields: map[string]interface{}{
						"name":        "Thing Two Copy",
						"description": "About Thing Two Copy",
					}}))
			})

			It("should point the alias to the third index", func() {
				Expect(conn.addAliasCalls.calls).To(Equal([][]interface{}{{"test", []string{"two"}}}))
			})

			It("should remove the alias to the first index", func() {
				Expect(conn.removeAliasCalls.calls).To(Equal([][]interface{}{{"test", []string{"zero"}}}))
			})
		})

		Context("When the first and last of three indices exist", func() {
			BeforeEach(func() {
				migrationSet.Migrations = migrations[0:3]
				conn.customIndicesExist = func(indices []string) (bool, error) {
					if indices[0] == "zero" || indices[0] == "two" {
						return true, nil
					}
					return false, nil
				}
				conn.createIndexCalls.returns = [][]interface{}{{&goes.Response{}, nil}}
				conn.forEachCalls.returns = [][]interface{}{
					{
						[]*goes.Document{
							{
								Index:  "zero",
								Type:   "things",
								Id:     "1",
								Fields: map[string]interface{}{"name": "Thing One"},
							},
							{
								Index:  "zero",
								Type:   "things",
								Id:     "2",
								Fields: map[string]interface{}{"name": "Thing Two"},
							},
						},
						nil,
					},
				}
				conn.indexCalls.returns = [][]interface{}{{&goes.Response{}, nil}, {&goes.Response{}, nil}, {&goes.Response{}, nil}, {&goes.Response{}, nil}}
			})

			It("should not return an error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("should not try to create any indices", func() {
				Expect(conn.createIndexCalls.calls).To(BeEmpty())
			})

			It("should not index any docs", func() {
				Expect(conn.indexCalls.calls).To(BeEmpty())
			})

			It("should not add an alias", func() {
				Expect(conn.addAliasCalls.calls).To(BeEmpty())
			})
		})

	})
})
