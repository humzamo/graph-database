package graph

import "errors"

var (
	ErrNoResults                 = errors.New("no results found from traversal")
	ErrLabelCannotBeEmpty        = errors.New("label cannot be empty")
	errNoEdgeLabels              = errors.New("no edge labels provided")
	errInvalidTraversalDirection = errors.New("invalid traversal direction")
	errDataInterfaceConversion   = errors.New("unable to convert data interface to map")
	errValueToStringConversion   = errors.New("unable to convert value to string")
	errKeyToStringConversion     = errors.New("unable to convert key to string")
	errGremlinTraversalTimeout   = errors.New("timeout waiting for gremlin traversal response")

	ErrNeptuneDisabled = errors.New("neptune not enabled")
)
