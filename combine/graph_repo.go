package main

import "database/sql"

// Graph repo
type GraphRepository struct{ db *sql.DB }
type graphNeighbor struct {
	ID    string
	Score float64
}

func NewGraphRepository(dbPath string) (*GraphRepository, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &GraphRepository{db: db}, nil
}
func (r *GraphRepository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}
func (r *GraphRepository) GetNeighbors(jobID string, limit int) ([]graphNeighbor, error) {
	rows, err := r.db.Query(`SELECT neighbor_id, score FROM job_neighbors WHERE job_id = ? ORDER BY score DESC LIMIT ?`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []graphNeighbor
	for rows.Next() {
		var n graphNeighbor
		if err := rows.Scan(&n.ID, &n.Score); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
