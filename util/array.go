package util

import (
	"math/rand"
)

func RandomInt64s(arrs []int64)  {
	if len(arrs) <= 1{
		return
	}

	for i := len(arrs) - 1; i > 0; i-- {
		num := rand.Intn(i + 1)
		arrs[i], arrs[num] = arrs[num], arrs[i]
	}
}
