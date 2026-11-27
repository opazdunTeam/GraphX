package postgres

import (
	"context"
	"fmt"

	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
)

func (s *InvestigationStore) ListSources(ctx context.Context) ([]investigations.CatalogSource, error) {
	rows, err := s.pool.Query(ctx, `
		select code, name, mode, enabled
		from sources
		where code = 'fixture'
	`)
	if err != nil {
		return nil, fmt.Errorf("query source catalog: %w", err)
	}
	defer rows.Close()

	var sources []investigations.CatalogSource
	for rows.Next() {
		var source investigations.CatalogSource
		if err := rows.Scan(&source.Code, &source.Name, &source.Mode, &source.Enabled); err != nil {
			return nil, fmt.Errorf("read source catalog: %w", err)
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source catalog: %w", err)
	}
	return sources, nil
}
