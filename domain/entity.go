package domain

type IndexDTO struct {
	IndexCreated IndexCreated `json:"$$indexCreated" gedb:"$$indexCreated,omitzero"`
	IndexRemoved string       `json:"$$indexRemoved" gedb:"$$indexRemoved,omitzero"`
}

type IndexCreated struct {
	FieldName   string  `json:"fieldName" gedb:"fieldName,omitzero"`
	Unique      bool    `json:"unique" gedb:"unique,omitzero"`
	Sparse      bool    `json:"sparse" gedb:"sparse,omitzero"`
	ExpireAfter float64 `json:"$$expireAfterSeconds" gedb:"$$expireAfterSeconds,omitzero"`
}

type Update struct {
	OldDoc Document
	NewDoc Document
}
