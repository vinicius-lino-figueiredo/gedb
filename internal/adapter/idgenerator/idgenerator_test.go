package idgenerator

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type IDGeneratorTestSuite struct {
	suite.Suite
	ig *IDGenerator
}

func (s *IDGeneratorTestSuite) SetupTest() {
	s.ig = NewIDGenerator().(*IDGenerator)
}

func (s *IDGeneratorTestSuite) TestLength() {
	id, err := s.ig.GenerateID(3)
	s.NoError(err)
	s.Len(id, 3)

	id, err = s.ig.GenerateID(16)
	s.NoError(err)
	s.Len(id, 16)

	id, err = s.ig.GenerateID(42)
	s.NoError(err)
	s.Len(id, 42)

	id, err = s.ig.GenerateID(1000)
	s.NoError(err)
	s.Len(id, 1000)
}

// If the value in the random reader does not repeat, IDs multiple times will
// not result in collision.
func (s *IDGeneratorTestSuite) TestCollision() {
	t := `abcdefghijklmnopqrstuvwxy0123456789ABCDEFGHIJKLMNOPQRSTUVWXYãẽĩñõũṽỹáćéǵíḱĺḿńóṕŕśúǘẃ`
	s.ig = NewIDGenerator(WithReader(strings.NewReader(t))).(*IDGenerator)

	id1, err := s.ig.GenerateID(56)
	s.NoError(err)
	s.Len(id1, 56)

	id2, err := s.ig.GenerateID(56)
	s.NoError(err)
	s.Len(id2, 56)

	s.NotEqual(id1, id2)
}

func (s *IDGeneratorTestSuite) TestReadError() {
	s.ig = NewIDGenerator(WithReader(strings.NewReader(""))).(*IDGenerator)

	id, err := s.ig.GenerateID(1)
	s.ErrorIs(err, io.EOF)
	s.Zero(id)
}

func TestIDGeneratorTestSuite(t *testing.T) {
	suite.Run(t, new(IDGeneratorTestSuite))
}
