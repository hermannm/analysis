package db

import (
	"hermannm.dev/enumnames"
)

type Operator uint8

const (
	OperatorOR Operator = iota + 1
	OperatorAND
)

var operatorNames = enumnames.NewMap(map[Operator]string{
	OperatorOR:  "OR",
	OperatorAND: "AND",
})

func (operator Operator) IsValid() bool {
	return operatorNames.ContainsEnumValue(operator)
}

func (operator Operator) String() string {
	return operatorNames.GetNameOrFallback(operator, "INVALID_OPERATOR")
}

func (operator Operator) MarshalJSON() ([]byte, error) {
	return operatorNames.MarshalToNameJSON(operator)
}

func (operator *Operator) UnmarshalJSON(bytes []byte) error {
	return operatorNames.UnmarshalFromNameJSON(bytes, operator)
}
