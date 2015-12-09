package migrate

// MigrationSet holds a list of Migration structs and other settings needed to do a migration.
type MigrationSet struct {
	IndexAlias string // If empty, no alias will be set.
	Migrations []Migration
}

// Run figures out which migrations from the set need to be run and runs them.
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

	lastM := s.Migrations[len(s.Migrations)-1]
	if exists, err := c.IndicesExist([]string{lastM.ToIndex}); err != nil {
		return err
	} else if exists {
		return nil // Last index exists, so no migration is needed.
	}

	composite := compositeMigration(s.Migrations[firstToRun:])
	if err := Migrate(c, composite); err != nil {
		return err
	}

	if _, err := c.AddAlias(s.IndexAlias, []string{composite.ToIndex}); err != nil {
		return err
	}
	if composite.FromIndex != "" {
		if _, err := c.RemoveAlias(s.IndexAlias, []string{composite.FromIndex}); err != nil {
			return err
		}
	}
	return nil
}
