package api

import (
	"hermannm.dev/analysis/db"
)

type AnalysisAPI struct {
	db db.AnalysisDatabase
}

func NewAnalysisAPI(db db.AnalysisDatabase) AnalysisAPI {
	return AnalysisAPI{db: db}
}
