package skiplist

import (
	"math"
	"testing"
)

func TestRandomLevelDistribution(t *testing.T) {
	numSamples := 1000000
	counts := make(map[int]int)
	for range numSamples {
		level := randomLevel()
		counts[level]++
	}

	// Check if the distribution is roughly geometric.
	// With P = 1/2, we expect the number of nodes at level i+1 to be
	// roughly half the number of nodes at level i.
	for i := 1; i < MaxLevel; i++ {
		count1 := counts[i]
		count2 := counts[i+1]

		// If we have a decent number of samples for level i,
		// we can check the ratio.
		if count1 > 100 {
			// The ratio should be around 0.5 (our P value).
			// We allow for some variance.
			ratio := float64(count2) / float64(count1)
			if math.Abs(ratio-P) > 0.1 {
				t.Errorf("Expected ratio between level %d and %d to be around %.2f, but got %.2f", i, i+1, P, ratio)
			}
		}
	}
}
