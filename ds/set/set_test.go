package set

import (
	"fmt"
	"testing"
)

type Object struct {
	Id   int
	Name string
}

func TestSet(t *testing.T) {

	s := New[(Object)]()

	for i := 0; i < 10; i++ {
		s.Add(Object{
			Id:   i,
			Name: fmt.Sprintf("now-%d", i),
		})
	}

	testCase := Object{
		Id:   0,
		Name: "now-0",
	}

	fmt.Println(s.Contains(testCase))

}
