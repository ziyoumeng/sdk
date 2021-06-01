package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPage(t *testing.T) {
	shouldSuccess := []struct {
		//in
		page     int32
		pageSize int32
		total    int

		//expected
		start int
		end   int
	}{
		{0, 0, 0, 0, 0},
		{0, 2, 4, 0, 0},
		{1, 2, 4, 0, 2},
		{2, 2, 4, 2, 4},
		{3, 2, 4, 0, 0},
	}
	for _, v := range shouldSuccess {
		start, end := Page(v.page, v.pageSize, v.total)
		assert.Equal(t, v.start, start)
		assert.Equal(t, v.end, end)
	}
}
