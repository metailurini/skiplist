package skiplist

import (
	"math"
	"testing"
)

func TestRandomLevelDistribution(t *testing.T) {
	numSamples := 1000000
	counts := make(map[int]int)
	rng := newRNGWithSeed(0x123456789abcdef)
	for range numSamples {
		level := rng.RandomLevel()
		counts[level]++
	}

	// Check if the distribution is roughly geometric.
	// With P = 1/2, we expect the number of nodes at level i+1 to be
	// roughly half the number of nodes at level i.
	for i := 1; i < MaxLevel; i++ {
		count1 := counts[i]
		if count1 == 0 {
			continue
		}

		count2 := counts[i+1]

		ratio := float64(count2) / float64(count1)

		// The number of nodes promoted from level i to i+1 follows a
		// Binomial(count1, P) distribution, so the ratio count2/count1
		// has mean P and variance P(1-P)/count1. We tolerate deviations
		// up to five standard deviations, which keeps the check tight
		// for the densely populated lower levels while avoiding
		// spurious failures once the sample sizes thin out.
		stdDev := math.Sqrt(P * (1 - P) / float64(count1))
		tolerance := 5 * stdDev

		if math.Abs(ratio-P) > tolerance {
			t.Errorf("Expected ratio between level %d and %d to be around %.2f ± %.4f, but got %.2f", i, i+1, P, tolerance, ratio)
		}
	}
}

func BenchmarkRNGPoolInit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		rng := newRNG()
		rng.nextRandom64() // This will trigger pool.New on first call
	}
}
