package migrate

import (
	"net/url"

	"github.com/belogik/goes"
)

// Connection includes the set of goes.Connection methods used to do migrations. Using this
// interface enables testing with a fake ES connection.
type Connection interface {
	IndicesExist([]string) (bool, error)
	CreateIndex(string, interface{}) (*goes.Response, error)
	Index(goes.Document, url.Values) (*goes.Response, error)
	ForEach(query interface{}, indexList []string, typeList []string, cb func(*goes.Document) error) error
	RemoveAlias(alias string, indices []string) (*goes.Response, error)
	AddAlias(alias string, indices []string) (*goes.Response, error)
	RefreshIndex(index string) (*goes.Response, error)
}

// ESConnection extends goes.Connection with ForEach method.
type ESConnection struct {
	*goes.Connection
	Timeout string
	Size    int
}

var _ Connection = &ESConnection{}

// NewESConnection creates a new ESConnection with default timeout and size.
func NewESConnection(host string, port string) *ESConnection {
	return &ESConnection{
		Connection: goes.NewConnection(host, port),
		Timeout:    "1m",
		Size:       100,
	}
}

// ForEach uses ES scan and scroll to call the specified function for every doc matching the query.
func (c *ESConnection) ForEach(query interface{}, indexList []string, typeList []string, cb func(*goes.Document) error) error {
	res, err := c.Scan(query, indexList, typeList, c.Timeout, c.Size)
	if err != nil {
		return err
	}

	doc := goes.Document{}

	res, err = c.Scroll(res.ScrollId, c.Timeout)
	for err == nil && len(res.Hits.Hits) > 0 {
		for _, hit := range res.Hits.Hits {
			doc.Index = hit.Index
			doc.Type = hit.Type
			doc.Id = hit.Id
			doc.Fields = hit.Source
			if err = cb(&doc); err != nil {
				return err
			}
		}
		res, err = c.Scroll(res.ScrollId, c.Timeout)
	}
	if err != nil {
		return err
	}
	return nil
}
