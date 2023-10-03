package db

import (
	"hermannm.dev/enumnames"
)

type Operator uint8

const (
	OperatorOR Operator = iota + 1
	OperatorAND
)

var operatorMap = enumnames.NewMap(map[Operator]string{
	OperatorOR:  "OR",
	OperatorAND: "AND",
})

func (operator Operator) IsValid() bool {
	return operatorMap.ContainsEnumValue(operator)
}

func (operator Operator) String() string {
	return operatorMap.GetNameOrFallback(operator, "INVALID_OPERATOR")
}

func (operator Operator) MarshalJSON() ([]byte, error) {
	return operatorMap.MarshalToNameJSON(operator)
}

func (operator *Operator) UnmarshalJSON(bytes []byte) error {
	return operatorMap.UnmarshalFromNameJSON(bytes, operator)
}
