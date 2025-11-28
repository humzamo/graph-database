package graph

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/pkg/errors"

	"github.com/humzamo/graph-database/pkg/logger"
)

// VertexCount returns the number of vertices in the graph.
// Useful for testing the connection to the graph.
func (s *GraphService) VertexCount(ctx context.Context) (int, error) {
	errMsg := "gremlin error counting vertices"
	logger := logger.NewLogger(ctx, s, "VertexCount")

	v := s.reader.gremlin.V().Count()
	res, err := returnResultsFromTraversal(v)
	if err != nil {
		return -1, errors.Wrap(err, errMsg)
	}
	count, err := res[0].GetInt()
	if err != nil {
		return -1, errors.Wrap(err, errMsg)
	}

	logger.Debug().Msgf("Vertex count: %v", count)
	return count, nil
}

// AddVertex adds a vertex to the graph with the given label and property.
// If a vertex with the same label and property already exists, it will not be added.
func (s *GraphService) AddVertex(ctx context.Context, label string, property Property) error {
	errMsg := "gremlin error adding vertex"
	logger := logger.NewLogger(ctx, s, "AddVertex")

	if label == "" {
		return errors.Wrap(ErrLabelCannotBeEmpty, errMsg)
	}

	v := s.writer.gremlin.V().
		Has(label, property.Key, property.Value).Fold().
		Coalesce(s.writer.gremlin.GetGraphTraversal().Unfold(),
			s.writer.gremlin.AddV(label).Property(property.Key, property.Value))

	err := traverseAndCheckError(v)
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	logger.Debug().Msgf("Added vertex with label %s and property %s", label, property)
	return nil
}

// GetVertices returns all vertices with the given label and properties.
// If no results are found, an empty slice is returned.
func (s *GraphService) GetVertices(ctx context.Context, label string, property ...Property) ([]Vertex, error) {
	errMsg := "gremlin error getting vertices"
	logger := logger.NewLogger(ctx, s, "GetVertices")

	if label == "" {
		return nil, errors.Wrap(ErrLabelCannotBeEmpty, errMsg)
	}

	v := s.reader.gremlin.V().HasLabel(label)
	for _, p := range property {
		v = v.Has(p.Key, p.Value)
	}
	v = v.ElementMap()

	results, err := returnResultsFromTraversal(v)
	if err != nil {
		return nil, errors.Wrap(err, errMsg)
	}

	vertices := []Vertex{}
	for _, r := range results {
		vertex, err := dataToVertex(r)
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert data to vertex")
		}
		vertices = append(vertices, vertex)
	}

	logger.Debug().Msgf("Found %d vertices with label %s and properties %v", len(vertices), label, property)
	return vertices, nil
}

// AddPropertiesToVertex adds properties to a vertex with the given label and existing properties.
// It can also update the value of an existing property (if the key already exists for the existing property).
func (s *GraphService) AddPropertiesToVertex(ctx context.Context, label string, existing, new []Property) error {
	errMsg := "gremlin error adding property to vertex"
	logger := logger.NewLogger(ctx, s, "AddPropertiesToVertex")

	existingV, err := s.GetVertices(ctx, label, existing...)
	if err != nil {
		return errors.Wrap(err, errMsg)
	}
	if len(existingV) == 0 {
		return errors.Wrap(ErrNoResults, errMsg)
	}

	v := s.writer.gremlin.V()
	for _, p := range existing {
		v = v.Has(label, p.Key, p.Value)
	}
	for _, p := range new {
		v = v.Property(p.Key, p.Value)
	}

	err = traverseAndCheckError(v)
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	logger.Debug().Msgf("Added properties to vertex with label %s and properties %v", label, new)
	return nil
}

// DeleteVertex deletes a vertex with the given label and properties.
// Assumes that the vertex already exists, otherwise do nothing.
func (s *GraphService) DeleteVertex(ctx context.Context, label string, properties ...Property) error {
	errMsg := "gremlin error deleting vertex"
	logger := logger.NewLogger(ctx, s, "DeleteVertex")

	if label == "" {
		return errors.Wrap(ErrLabelCannotBeEmpty, errMsg)
	}

	v := s.writer.gremlin.V()
	for _, p := range properties {
		v = v.Has(label, p.Key, p.Value)
	}
	v = v.Drop()

	err := traverseAndCheckError(v)
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	logger.Debug().Msgf("Deleted vertex with label %s and properties %v", label, properties)
	return nil
}

// AddEdge adds an edge between two vertices with the given labels and properties.
// Assumes that the vertices already exist, otherwise do nothing.
func (s *GraphService) AddEdge(ctx context.Context, outLabel string, out Property, inLabel string, in Property, edgeLabel string) error {
	errMsg := "gremlin error adding edge"
	logger := logger.NewLogger(ctx, s, "AddEdge")

	if outLabel == "" || inLabel == "" {
		return errors.Wrap(ErrLabelCannotBeEmpty, errMsg)
	}

	v := s.writer.gremlin.V().
		Has(outLabel, out.Key, out.Value).
		As("a").
		V().Has(inLabel, in.Key, in.Value).
		AddE(edgeLabel).
		From("a")

	err := traverseAndCheckError(v)
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	logger.Debug().Msgf("Added edge from vertex with label %s and property %s to vertex with label %s and property %s",
		outLabel, out, inLabel, in)
	return nil
}

// RemoveEdge removes an edge between two vertices with the given labels and properties.
// Assumes that the vertices and the edge already exist, otherwise do nothing.
func (s *GraphService) RemoveEdge(ctx context.Context, outLabel string, out Property, inLabel string, in Property, edgeLabel string) error {
	errMsg := "gremlin error removing edge"
	logger := logger.NewLogger(ctx, s, "RemoveEdge")

	if outLabel == "" || inLabel == "" {
		return errors.Wrap(ErrLabelCannotBeEmpty, errMsg)
	}

	v := s.writer.gremlin.V().
		Has(outLabel, out.Key, out.Value).
		OutE(edgeLabel).
		Where(s.writer.gremlin.GetGraphTraversal().
			InV().Has(inLabel, in.Key, in.Value)).
		Drop()

	err := traverseAndCheckError(v)
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	logger.Debug().Msgf("Removed edge from vertex with label %s and property %s to vertex with label %s and property %s",
		outLabel, out, inLabel, in)
	return nil
}

// GetVerticesByEdges traverses the graph starting from a vertex with the given label and properties,
// and traverses through the given edge labels. The edge labels are traversed in order.
//
// The traversal direction allows traversal from A to C with via edges directed as follows:
// OUT : A->B->C (i.e. explicitly with edges from A to B and B to C)
// IN : A<-B<-C (i.e. explicitly with edges from B to A and C to B)
// BOTH : A->B<-C (i.e. edges can be in either direction)
func (s *GraphService) GetVerticesByEdges(ctx context.Context, direction TraversalDirection, fromVertexLabel string, fromVertexProperties []Property, edgeLabels []string) ([]Vertex, error) {
	errMsg := "gremlin error traversing request"
	logger := logger.NewLogger(ctx, s, "GetVerticesByEdges")

	if fromVertexLabel == "" {
		return nil, errors.Wrap(ErrLabelCannotBeEmpty, errMsg)
	}

	v := s.reader.gremlin.V().HasLabel(fromVertexLabel)
	for _, p := range fromVertexProperties {
		v = v.Has(p.Key, p.Value)
	}

	v, err := setTraversalPath(v, direction, edgeLabels)
	if err != nil {
		return nil, errors.Wrap(err, errMsg)
	}

	v = v.ElementMap()

	results, err := returnResultsFromTraversal(v)
	if err != nil {
		return nil, errors.Wrap(err, errMsg)
	}

	vertices := []Vertex{}
	for _, r := range results {
		vertex, err := dataToVertex(r)
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert data to vertex")
		}
		vertices = append(vertices, vertex)
	}

	logger.Debug().Msgf("Found %d vertices from traversal", len(vertices))
	return vertices, nil
}

func setTraversalPath(v *gremlingo.GraphTraversal, direction TraversalDirection, edgeLabels []string) (*gremlingo.GraphTraversal, error) {
	if len(edgeLabels) == 0 {
		return nil, errNoEdgeLabels
	}

	switch direction {
	case TraversalDirectionOut:
		for _, e := range edgeLabels {
			v = v.Out(e)
		}
	case TraversalDirectionIn:
		for _, e := range edgeLabels {
			v = v.In(e)
		}
	case TraversalDirectionBoth:
		for _, e := range edgeLabels {
			v = v.Both(e)
		}
	default:
		return nil, errInvalidTraversalDirection
	}

	return v, nil
}

func dataToVertex(r *gremlingo.Result) (Vertex, error) {
	data, ok := r.Data.(map[any]any)
	if !ok {
		return Vertex{}, errDataInterfaceConversion
	}

	vertex := Vertex{
		Properties: make(map[string]any),
	}
	for k, v := range data {
		key, ok := k.(string)
		if !ok {
			return Vertex{}, errKeyToStringConversion
		}
		switch key {
		case idField:
			// This annoying conversion is because the id is a different type in the graph databases
			// for Gremlin Server and Neptune.
			idInt64, ok := v.(int64)
			if ok {
				vertex.Id = strconv.Itoa(int(idInt64))
				continue
			}
			idInt, ok := v.(int)
			if ok {
				vertex.Id = strconv.Itoa(idInt)
				continue
			}
			idStr, ok := v.(string)
			if ok {
				vertex.Id = idStr
				continue
			}
			return Vertex{}, fmt.Errorf("unable to convert value %v of type %v to string", v, reflect.TypeOf(v))
		case labelField:
			label, ok := v.(string)
			if !ok {
				return Vertex{}, errValueToStringConversion
			}
			vertex.Label = label
		default:
			vertex.Properties[key] = v
		}
	}
	return vertex, nil
}

func returnResultsFromTraversal(v *gremlingo.GraphTraversal) ([]*gremlingo.Result, error) {
	results, err := v.ToList()
	if err != nil {
		return nil, err
	}
	return results, nil
}

func traverseAndCheckError(v *gremlingo.GraphTraversal) error {
	prom := v.Iterate()

	select {
	case err := <-prom:
		return err
	case <-time.After(time.Second * 5):
		return errGremlinTraversalTimeout
	}
}
