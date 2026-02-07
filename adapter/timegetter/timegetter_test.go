package timegetter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// Completely unnecessary test suite because TimeGetter actually just returns
// time.Now(), but why not? Otherwise it would be missing in code coverage
// anyways, so it will be added.
type TimeGetterTestSuite struct {
	suite.Suite
	tg *TimeGetter
}

func (s *TimeGetterTestSuite) SetupTest() {
	s.tg = NewTimeGetter().(*TimeGetter)
}

func (s *TimeGetterTestSuite) TestGetTime() {
	before := time.Now()

	result := s.tg.GetTime()

	after := time.Now()

	s.NotZero(result)
	s.GreaterOrEqual(result, before)
	s.LessOrEqual(result, after)
}

func (s *TimeGetterTestSuite) TestGetTimeConsistency() {
	time1 := s.tg.GetTime()
	time.Sleep(1 * time.Millisecond)
	time2 := s.tg.GetTime()

	s.True(time2.After(time1))
}

func TestTimeGetterTestSuite(t *testing.T) {
	suite.Run(t, new(TimeGetterTestSuite))
}
