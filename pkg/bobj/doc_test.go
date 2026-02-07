package bobj_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/bobj"
)

type DocTestSuite struct {
	suite.Suite
}

func (s *DocTestSuite) TestNewDoc() {
	for _, tc := range newDocumentCases {
		s.Run(tc.name, func() {
			obj, err := bobj.NewDocument(tc.input)
			if tc.wantErr {
				if !s.Error(err) {
					return
				}
				s.Nil(obj)
			} else {
				if !s.NoError(err) {
					return
				}
				s.NotNil(obj)
			}
		})
	}
}

func (s *DocTestSuite) TestGetOk() {
	for _, tc := range objectGetCases {
		s.Run(tc.name, func() {
			obj, err := bobj.NewDocument(tc.input)
			if !s.NoError(err) {
				return
			}
			v, ok := obj.GetOk(tc.key)
			if !tc.wantFound {
				s.False(ok)
				s.Nil(v)
				return
			}
			if !s.True(ok) {
				return
			}
		})
	}
}

func TestDocTestSuite(t *testing.T) {
	suite.Run(t, new(DocTestSuite))
}
