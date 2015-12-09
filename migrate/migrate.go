package migrate

import (
	"fmt"

	"github.com/belogik/goes"
)

// Migrate attempts to perform the Migration on the given Connection.
func Migrate(c Connection, m *Migration) error {
	if m.FromIndex != "" {
		if exists, err := c.IndicesExist([]string{m.FromIndex}); err != nil {
			return err
		} else if !exists {
			return fmt.Errorf("Source index %s doesn't exist, cannot migrate.", m.FromIndex)
		}
	}

	if exists, err := c.IndicesExist([]string{m.ToIndex}); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("Destination index %s already exists, cannot migrate.", m.ToIndex)
	}

	if _, err := c.CreateIndex(m.ToIndex, m.Settings); err != nil {
		return err
	}

	if m.FromIndex == "" {
		return nil
	}

	c.ForEach(map[string]interface{}{"match_all": map[string]string{}}, []string{m.FromIndex}, nil, func(doc *goes.Document) error {
		newDocs := m.Transform(doc)
		for _, newDoc := range newDocs {
			newDoc.Index = m.ToIndex
			newDoc.BulkCommand = "index"
			if _, err := c.Index(*newDoc, nil); err != nil {
				return err
			}
		}
		return nil
	})

	return nil
}
