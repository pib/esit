package migrate_test

import (
	"net/url"

	"github.com/belogik/goes"
	"github.com/pib/esit/migrate"
)

type mockCalls struct {
	calls   [][]interface{}
	returns [][]interface{}
}

func (m *mockCalls) call(args ...interface{}) []interface{} {
	if len(m.returns) == 0 {
		panic("Called mocked function without first setting responses")
	}
	ret := m.returns[0]
	m.returns = m.returns[1:]
	m.calls = append(m.calls, args)
	return ret
}

type mockConnection struct {
	indicesExistCalls mockCalls
	createIndexCalls  mockCalls
	indexCalls        mockCalls
	forEachCalls      mockCalls
}

var _ migrate.Connection = &mockConnection{} // Check that mockConnection implements migrate.Connection

func (c *mockConnection) IndicesExist(indices []string) (bool, error) {
	ret := c.indicesExistCalls.call(indices)
	return ret[0].(bool), errOrNil(ret[1])
}

func (c *mockConnection) CreateIndex(index string, settings interface{}) (*goes.Response, error) {
	ret := c.createIndexCalls.call(index, settings)
	return ret[0].(*goes.Response), errOrNil(ret[1])
}

func (c *mockConnection) Index(d goes.Document, extraArgs url.Values) (*goes.Response, error) {
	ret := c.indexCalls.call(d, extraArgs)
	return ret[0].(*goes.Response), errOrNil(ret[1])
}

// "returns" for the forEachCalls should actually be an array of *goes.Document objects to pass to
// the callback and also an error or nil for the actual return value.
func (c *mockConnection) ForEach(query interface{}, indexList []string, typeList []string, cb func(*goes.Document) error) error {
	ret := c.forEachCalls.call(query, indexList, typeList, cb)
	cbArgs := ret[0].([]*goes.Document)
	for _, arg := range cbArgs {
		if err := cb(arg); err != nil {
			return err
		}
	}
	return errOrNil(ret[1])
}

func errOrNil(err interface{}) error {
	if err == nil {
		return nil
	}
	return err.(error)
}
