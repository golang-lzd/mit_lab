package main

import (
	"6.824/mr"
	"fmt"
	"strings"
	"unicode"
)

func main() {
	s := "******* This1 file should be named 844.txt or 844.zip *******"
	fmt.Println(Map("", s))

}

func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}
