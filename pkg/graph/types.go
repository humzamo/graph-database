package graph

const (
	idField    = "id"
	labelField = "label"
)

type Vertex struct {
	Id         string
	Label      string
	Properties map[string]any
}

type Property struct {
	Key   string
	Value any
}

type TraversalDirection string

var (
	TraversalDirectionOut  TraversalDirection = "OUT"
	TraversalDirectionIn   TraversalDirection = "IN"
	TraversalDirectionBoth TraversalDirection = "BOTH"
)
