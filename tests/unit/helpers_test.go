package unit

import (
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// uuidMustParse parses a uuid in test data.
func uuidMustParse(t *testing.T, raw string) uuid.UUID {
	t.Helper()
	parsed, err := uuid.Parse(raw)
	require.NoError(t, err)
	return parsed
}

// metricValue returns the value of name with exactly the given labels.
func metricValue(t *testing.T, gatherer prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if !sameLabels(metric.GetLabel(), labels) {
				continue
			}
			switch {
			case metric.GetCounter() != nil:
				return metric.GetCounter().GetValue()
			case metric.GetGauge() != nil:
				return metric.GetGauge().GetValue()
			case metric.GetHistogram() != nil:
				return float64(metric.GetHistogram().GetSampleCount())
			}
		}
	}
	return 0
}

// sameLabels reports whether the metric carries exactly labels.
func sameLabels(pairs []*dto.LabelPair, labels map[string]string) bool {
	if len(pairs) != len(labels) {
		return false
	}
	for _, pair := range pairs {
		expected, ok := labels[pair.GetName()]
		if !ok || expected != pair.GetValue() {
			return false
		}
	}
	return true
}

// metricFamily reports whether the gatherer exposes name at all.
func metricFamily(t *testing.T, gatherer prometheus.Gatherer, name string) bool {
	t.Helper()
	families, err := gatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name {
			return true
		}
	}
	return false
}
