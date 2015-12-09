package migrate_test

import (
	"errors"

	"github.com/belogik/goes"
	"github.com/pib/esit/migrate"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Migrate", func() {
	var conn *mockConnection
	var migration *migrate.Migration

	BeforeEach(func() {
		conn = new(mockConnection)
		migration = &migrate.Migration{FromIndex: "fram", ToIndex: "tu"}
	})

	Context("When no indices exist", func() {
		BeforeEach(func() {
			conn.indicesExistCalls.returns = [][]interface{}{
				{false, nil}, {false, nil}, // Allow two calls to IndicesExist, return false for both
			}
		})

		It("should return an error without even attempting to create an index", func() {
			Expect(migrate.Migrate(conn, migration)).To(HaveOccurred())
			Expect(conn.createIndexCalls.calls).To(BeEmpty())
		})
	})

	Context("When the destination index exists", func() {
		BeforeEach(func() {
			conn.indicesExistCalls.returns = [][]interface{}{
				{true, nil}, {true, nil}, // Allow two calls to IndicesExist, true for both.
			}
		})

		It("should return an error without even attempting to create an index", func() {
			Expect(migrate.Migrate(conn, migration)).To(HaveOccurred())
			Expect(conn.createIndexCalls.calls).To(BeEmpty())
		})
	})

	Context("When there are errors checking that the index exists", func() {
		BeforeEach(func() {
			conn.indicesExistCalls.returns = [][]interface{}{
				{false, errors.New("Failed for some reason")}, {true, nil}, {false, errors.New("Failed for some other reason")},
			}
		})

		It("should return an error without even attempting to create an index", func() {
			Expect(migrate.Migrate(conn, migration)).To(HaveOccurred())
			Expect(conn.createIndexCalls.calls).To(BeEmpty())

			Expect(migrate.Migrate(conn, migration)).To(HaveOccurred())
			Expect(conn.createIndexCalls.calls).To(BeEmpty())
		})
	})

	Context("When there is an error creating the index", func() {
		BeforeEach(func() {
			conn.indicesExistCalls.returns = [][]interface{}{
				{true, nil}, {false, nil},
			}
			conn.createIndexCalls.returns = [][]interface{}{
				{&goes.Response{}, errors.New("Failed to create somehow")},
			}
		})

		It("should return the error it received from goes", func() {
			Expect(migrate.Migrate(conn, migration)).To(MatchError("Failed to create somehow"))
		})
	})

	Context("When doing a migration with no TypeTransformers", func() {
		BeforeEach(func() {
			conn.indicesExistCalls.returns = [][]interface{}{
				{true, nil}, {false, nil},
			}
			conn.createIndexCalls.returns = [][]interface{}{
				{&goes.Response{}, nil},
			}
			conn.indexCalls.returns = [][]interface{}{
				{&goes.Response{}, nil}, {&goes.Response{}, nil}, {&goes.Response{}, nil},
			}
			conn.forEachCalls.returns = [][]interface{}{
				{
					[]*goes.Document{
						{
							Index:  "fram",
							Type:   "foo",
							Id:     "bar",
							Fields: map[string]interface{}{"howdy": "there"},
						},
						{
							Index:  "fram",
							Type:   "foo",
							Id:     "abc",
							Fields: map[string]interface{}{"def": "ghi"},
						},
					},
					nil,
				},
			}
		})

		It("should copy the documents from the source to the destination index", func() {
			migrate.Migrate(conn, migration)
			Expect(conn.indexCalls.calls[0][0]).To(Equal(
				goes.Document{
					Index:       "tu",
					Type:        "foo",
					Id:          "bar",
					BulkCommand: "index",
					Fields:      map[string]interface{}{"howdy": "there"},
				},
			))
			Expect(conn.indexCalls.calls[1][0]).To(Equal(
				goes.Document{
					Index:       "tu",
					Type:        "foo",
					Id:          "abc",
					BulkCommand: "index",
					Fields:      map[string]interface{}{"def": "ghi"},
				},
			))

		})
	})

	Context("When doing a migration with TypeTransformers", func() {
		BeforeEach(func() {
			migration.TypeTransformers = map[string]func(*goes.Document) []*goes.Document{
				"foo": func(doc *goes.Document) []*goes.Document {
					doc.Fields.(map[string]interface{})["extra"] = "stuff"
					return []*goes.Document{doc}
				},
				"bar": migrate.DropAllTransformer,
			}
			conn.indicesExistCalls.returns = [][]interface{}{
				{true, nil}, {false, nil},
			}
			conn.createIndexCalls.returns = [][]interface{}{
				{&goes.Response{}, nil},
			}
			conn.indexCalls.returns = [][]interface{}{
				{&goes.Response{}, nil}, {&goes.Response{}, nil}, {&goes.Response{}, nil},
			}
			conn.forEachCalls.returns = [][]interface{}{
				{
					[]*goes.Document{
						{
							Index:  "fram",
							Type:   "foo",
							Id:     "bar",
							Fields: map[string]interface{}{"howdy": "there"},
						},
						{
							Index:  "fram",
							Type:   "bar",
							Id:     "abc",
							Fields: map[string]interface{}{"def": "ghi"},
						},
					},
					nil,
				},
			}
		})

		It("should copy the documents according to the TypeTransformers", func() {
			migrate.Migrate(conn, migration)
			Expect(conn.indexCalls.calls[0][0]).To(Equal(
				goes.Document{
					Index:       "tu",
					Type:        "foo",
					Id:          "bar",
					BulkCommand: "index",
					Fields:      map[string]interface{}{"howdy": "there", "extra": "stuff"},
				},
			))
			Expect(conn.indexCalls.calls).To(HaveLen(1))
		})
	})

})
