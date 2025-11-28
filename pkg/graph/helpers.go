package graph

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"
)

// ConvertVertexProperty converts the property into a specified type
// for a given vertex, label, and key.
func ConvertVertexProperty[T any](v Vertex, label, key string) (T, error) {
	if v.Label != label {
		return *new(T),
			fmt.Errorf("expected label %s, got %s", label, v.Label)
	}
	property, ok := v.Properties[key]
	if !ok {
		return *new(T),
			fmt.Errorf("expected property %s in vertex", key)
	}
	converted, ok := property.(T)
	if !ok {
		return *new(T),
			fmt.Errorf("expected property %s to be %T, got %T", key, *new(T), property)
	}
	return converted, nil
}

// GenerateDummyUUIDs generates a slice of dummy UUIDs.
// Useful for testing purposes when adding dummy data to Neptune.
func GenerateDummyUUIDs(count int) []string {
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		ids = append(ids, uuid.NewString())
	}
	return ids
}

// GenerateDummyVertices generates a slice of dummy vertices, all with
// a given label and key for the properties.
// Useful for testing purposes when adding dummy data to Neptune.
func GenerateDummyVertices[T any](label string, key string, vals []T) []Vertex {
	vertices := make([]Vertex, 0, len(vals))
	for i, val := range vals {
		vertices = append(vertices, Vertex{
			Id:    strconv.Itoa(i),
			Label: label,
			Properties: map[string]any{
				key: val,
			},
		})
	}
	return vertices
}
