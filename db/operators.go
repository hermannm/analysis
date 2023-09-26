package db

import (
	"encoding/json"
	"errors"
)

type Operator uint8

const (
	OperatorOR  Operator = 1
	OperatorAND Operator = 2
)

var operatorNames = map[Operator]string{
	OperatorOR:  "OR",
	OperatorAND: "AND",
}

func (operator Operator) IsValid() bool {
	_, ok := operatorNames[operator]
	return ok
}

func (operator Operator) String() string {
	if name, ok := operatorNames[operator]; ok {
		return name
	} else {
		return "[INVALID OPERATOR]"
	}
}

func (operator Operator) MarshalJSON() ([]byte, error) {
	if name, ok := operatorNames[operator]; ok {
		return json.Marshal(name)
	} else {
		return nil, errors.New("unrecognized operator")
	}
}

func (operator *Operator) UnmarshalJSON(bytes []byte) error {
	for candidate, name := range operatorNames {
		if name == string(bytes) {
			*operator = candidate
			return nil
		}
	}

	return errors.New("unrecognized operator")
}
