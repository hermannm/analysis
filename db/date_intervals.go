package db

import "hermannm.dev/enumnames"

type DateInterval int8

const (
	DateIntervalYear DateInterval = iota + 1
	DateIntervalQuarter
	DateIntervalMonth
	DateIntervalWeek
	DateIntervalDay
)

var dateIntervalMap = enumnames.NewMap(map[DateInterval]string{
	DateIntervalYear:    "YEAR",
	DateIntervalQuarter: "QUARTER",
	DateIntervalMonth:   "MONTH",
	DateIntervalWeek:    "WEEK",
	DateIntervalDay:     "DAY",
})

func (dateInterval DateInterval) IsValid() bool {
	return dateIntervalMap.ContainsEnumValue(dateInterval)
}

func (dateInterval DateInterval) String() string {
	return dateIntervalMap.GetNameOrFallback(dateInterval, "INVALID_DATE_INTERVAL")
}

func (dateInterval DateInterval) MarshalJSON() ([]byte, error) {
	return dateIntervalMap.MarshalToNameJSON(dateInterval)
}

func (dateInterval *DateInterval) UnmarshalJSON(bytes []byte) error {
	return dateIntervalMap.UnmarshalFromNameJSON(bytes, dateInterval)
}
