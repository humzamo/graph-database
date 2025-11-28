package graph

import (
	"context"
	"fmt"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/pkg/errors"

	"github.com/humzamo/graph-database/pkg/config"
	"github.com/humzamo/graph-database/pkg/logger"
)

type GraphConnection struct {
	conn    *gremlingo.DriverRemoteConnection
	gremlin *gremlingo.GraphTraversalSource
}

type GraphService struct {
	reader         *GraphConnection
	writer         *GraphConnection
	appEnvironment string
	appVersion     string
	debug          bool
}

func (s *GraphService) Name() string {
	return serviceStr
}

func (s *GraphService) AppEnvironment() string {
	return s.appEnvironment
}

func (s *GraphService) AppVersion() string {
	return s.appVersion
}

func (s *GraphService) IsDebug() bool {
	return s.debug
}

const serviceStr = "GraphService"

// NewGraphService creates a new graph service via Gremlin to the graph,
// using the reader and writer endpoints from the config.
// The caller is responsible for closing the connection.
func NewGraphService(ctx context.Context, cfg *config.Config) (*GraphService, error) {
	logger := logger.NewBaseLogger(ctx, serviceStr, "NewGraphService")

	logger.Info().Msg("Connecting to Gremlin Server...")

	readerEndpoint := fmt.Sprintf("%s:8182/gremlin", cfg.Neptune.ReaderEndpoint)
	readerRemoteConnection, err := gremlingo.NewDriverRemoteConnection(readerEndpoint)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to reader endpoint of Gremlin Server")
	}

	writerEndpoint := fmt.Sprintf("%s:8182/gremlin", cfg.Neptune.WriterEndpoint)
	writerRemoteConnection, err := gremlingo.NewDriverRemoteConnection(writerEndpoint)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to writer endpoint of Gremlin Server")
	}

	logger.Info().Msg("Connected to Gremlin Server")

	return &GraphService{
		reader: &GraphConnection{
			conn:    readerRemoteConnection,
			gremlin: gremlingo.Traversal_().With(readerRemoteConnection),
		},
		writer: &GraphConnection{
			conn:    writerRemoteConnection,
			gremlin: gremlingo.Traversal_().With(writerRemoteConnection),
		},
		appVersion:     cfg.AppVersion,
		appEnvironment: cfg.Environment,
		debug:          cfg.Debug,
	}, nil
}

// Close closes the connection to the reader and writer endpoints of the Gremlin server.
// This is safe to call on a nil or already-closed connection.
func (s *GraphService) Close() {
	if s == nil {
		return
	}

	if s.reader.conn != nil {
		s.reader.conn.Close()
	}

	if s.writer.conn != nil {
		s.writer.conn.Close()
	}
}
