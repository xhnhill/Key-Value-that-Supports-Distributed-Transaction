package test

import (
	"fmt"
	"math/big"
	"testing"
)

func TestBigAdd(t *testing.T) {
	//create a byte slice of size 4
	arr := make([]byte, 4)
	for i := 0; i < 4; i++ {
		arr[i] = 0xFF
	}
	x := new(big.Int).SetBytes(arr)
	q := new(big.Int).Div(x, big.NewInt(int64(4)))
	sum := big.NewInt(0)
	for i := 0; i < 4; i++ {
		fmt.Println(sum)
		sum.Add(sum, q)
	}

}
