package service

import (
	"strings"
)

func TyphoeusHashToArray(key string, values map[string][]string) []string {
	result := []string{}

	for k, val := range values {
		if strings.HasPrefix(k, key) {
			result = append(result, val[0])
		}
	}
	return result
}
