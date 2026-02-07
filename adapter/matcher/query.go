package matcher

// Numeric representations of supported logic operators.
const (
	And uint8 = iota
	Or
	Not
	Where
)

// Numeric representations of supported operators.
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

// Query stores a database query in a typed and easier to iterate struct.
type Query struct {
	Sub bool
	Lo  []LogicOp
}

// LogicOp stores a logic operator (and, or, not) and its children, which can be
// either a set of rules or a nested set of LogicOps.
type LogicOp struct {
	Type  uint8
	Rules []FieldRule
	Sub   []LogicOp
	Where *func(v any) (bool, error)
}

// FieldRule stores a set of conditions used to match a given object field.
type FieldRule struct {
	Addr  []string
	Conds []Cond
}

// Cond stores a single operation on a document field (such as $gt, $size).
type Cond struct {
	Op   uint8
	Val  any
	Ok   bool
	size int
}
