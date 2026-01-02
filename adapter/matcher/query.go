package matcher

// TODO
const (
	And uint8 = iota
	Or
	Not
	Where
)

// TODO
const (
	Eq uint8 = iota
	Ne
	Exists
	Lt
	Lte
	Gt
	Gte
	Size
	In
	Nin
	ElemMatch
	Regex
)

// Query TODO
type Query struct {
	Sub bool
	Lo  []LogicOp
}

// LogicOp TODO
type LogicOp struct {
	Type  uint8
	Rules []FieldRule
	Sub   []LogicOp
	Where *func(v any) (bool, error)
}

// FieldRule TODO
type FieldRule struct {
	Addr  []string
	Conds []Cond
}

// Cond TODO
type Cond struct {
	Op   uint8
	Val  any
	Ok   bool
	size int
}
