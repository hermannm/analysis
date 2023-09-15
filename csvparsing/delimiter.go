package csvparsing

import (
	"bufio"
	"io"

	"hermannm.dev/wrap"
)

var DefaultDelimitersToCheck = []rune{',', ';', '\t', ' ', '|'}

func DeduceFieldDelimiter(
	csvFile io.ReadSeeker, maxRowsToCheck int, delimitersToCheck []rune,
) (rune, error) {
	if len(delimitersToCheck) == 0 {
		delimitersToCheck = DefaultDelimitersToCheck
	}

	candidates := newDelimiterCandidateList(delimitersToCheck)

	scanner := bufio.NewScanner(csvFile)
	for i := 0; scanner.Scan() && i < maxRowsToCheck; i++ {
		line := scanner.Text()

		for i, candidate := range candidates {
			candidate.updateCounts(line)
			candidates[i] = candidate
		}
	}

	bestCandidate := candidates.getBestCandidate()

	// Resets reader position in file, so its data can be read subsequently
	if _, err := csvFile.Seek(0, io.SeekStart); err != nil {
		return bestCandidate, wrap.Error(
			err, "failed to reset CSV file after deducing field delimiter from it",
		)
	}

	return bestCandidate, nil
}

type delimiterCandidate struct {
	delimiter    rune
	highestCount int
	lowestCount  int
}

func (candidate *delimiterCandidate) updateCounts(line string) {
	count := 0
	for _, char := range line {
		if char == candidate.delimiter {
			count++
		}
	}

	if candidate.highestCount == -1 || candidate.highestCount < count {
		candidate.highestCount = count
	}
	if candidate.lowestCount == -1 || candidate.lowestCount > count {
		candidate.lowestCount = count
	}
}

type delimiterCandidateList []delimiterCandidate

func newDelimiterCandidateList(delimitersToCheck []rune) delimiterCandidateList {
	list := make([]delimiterCandidate, 0, len(delimitersToCheck))

	for _, delimiter := range delimitersToCheck {
		list = append(list, delimiterCandidate{
			delimiter:    delimiter,
			highestCount: -1,
			lowestCount:  -1,
		})
	}

	return list
}

func (list delimiterCandidateList) getBestCandidate() rune {
	var bestCandidate delimiterCandidate

	for _, candidate := range list {
		equalHighLow := candidate.highestCount == candidate.lowestCount
		bestEqualHighLow := bestCandidate.highestCount == bestCandidate.lowestCount
		higherThanBest := candidate.highestCount > bestCandidate.highestCount

		equalAndHigher := equalHighLow && bestEqualHighLow && higherThanBest

		moreEqual := equalHighLow && !bestEqualHighLow && candidate.highestCount > 0

		unequalButHigher := !equalHighLow && !bestEqualHighLow &&
			candidate.highestCount > bestCandidate.highestCount &&
			(candidate.lowestCount != 0 || bestCandidate.lowestCount == 0)

		if equalAndHigher || moreEqual || unequalButHigher {
			bestCandidate = candidate
		}
	}

	return bestCandidate.delimiter
}
