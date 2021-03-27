package gosimstor


import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func FromStringToString(data string) (interface{}, error) {
	return data, nil
}

func FromStringToFloat64Vector(data string) (interface{}, error) {
	var (
		split  = strings.Split(data, ",")
		vector = make([]float64, 0)
	)
	for i := 0; i < len(split); i++ {
		element, err := strconv.ParseFloat(split[i], 64)
		if err != nil {
			return nil, err
		}
		vector = append(vector, element)
	}
	return vector, nil
}

func ToStringString(data interface{}) (string, error) {
	var (
		convert, ok = data.(string)
	)
	if !ok {
		return convert,  errors.New("DOESN'T CONVERT 'STRING' TO STRING")
	}
	return convert,  nil
}

func ToStringFloat64Vector(data interface{}) (string,  error) {
	var (
		convert    string
		elements   = make([]string, 0)
		vector, ok = data.([]float64)
	)
	if !ok {
		return convert, errors.New("DOESN'T CONVERT 'FLOAT64 VECTOR' TO STRING")
	}
	for i := 0; i < len(vector); i++ {
		elements = append(
			elements,
			fmt.Sprintf("%f", vector[i]),
		)
	}
	convert = strings.Join(elements, ",")
	return convert,  nil
}

