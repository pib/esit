package migrate

// MigrationSet holds a list of Migration structs and other settings needed to do a migration.
type MigrationSet struct {
	IndexAlias string // If empty, no alias will be set.
	Migrations []Migration
}

func (s *MigrationSet) Run(c Connection) error {
	var firstToRun int

	// First to run is the last migration whose FromIndex exists but ToIndex doesn't.
	for i, m := range s.Migrations {
		if m.FromIndex == "" {
			continue
		}
		if exists, err := c.IndicesExist([]string{m.FromIndex}); err != nil {
			return err
		} else if exists {
			if exists, err := c.IndicesExist([]string{m.ToIndex}); err != nil {
				return err
			} else if !exists {
				firstToRun = i
			}
		}
	}

	composite := compositeMigration(s.Migrations[firstToRun:])

	Migrate(c, composite)
	return nil
}
