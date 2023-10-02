package db

import "hermannm.dev/enumnames"

type DateInterval uint8

const (
	DateIntervalYear DateInterval = iota + 1
	DateIntervalQuarter
	DateIntervalMonth
	DateIntervalWeek
	DateIntervalDay
)

var dateIntervalNames = enumnames.NewMap(map[DateInterval]string{
	DateIntervalYear:    "YEAR",
	DateIntervalQuarter: "QUARTER",
	DateIntervalMonth:   "MONTH",
	DateIntervalWeek:    "WEEK",
	DateIntervalDay:     "DAY",
})

func (dateInterval DateInterval) IsValid() bool {
	return dateIntervalNames.ContainsEnumValue(dateInterval)
}

func (dateInterval DateInterval) String() string {
	return dateIntervalNames.GetNameOrFallback(dateInterval, "INVALID_DATE_INTERVAL")
}

func (dateInterval DateInterval) MarshalJSON() ([]byte, error) {
	return dateIntervalNames.MarshalToNameJSON(dateInterval)
}

func (dateInterval *DateInterval) UnmarshalJSON(bytes []byte) error {
	return dateIntervalNames.UnmarshalFromNameJSON(bytes, dateInterval)
}
