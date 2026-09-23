package main

import (
	"errors"
	"testing"
)

func TestTrialOutcomeRejectsInvalidOrFailedTrials(t *testing.T) {
	for _, test := range []struct {
		name        string
		measurement measurement
		err         error
		status      string
		code        int
	}{
		{name: "completed", status: "completed", code: 0},
		{name: "trial failure", err: errors.New("relay failed"), status: "failed", code: 1},
		{name: "sample overflow", measurement: measurement{Validity: validityMeasurement{InputSampleBufferOverflow: true}}, status: "invalid", code: 1},
		{name: "missed workload", measurement: measurement{Generator: generatorMeasurement{Unable: true}}, status: "invalid", code: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			status, _, code := trialOutcome(test.measurement, test.err)
			if status != test.status || code != test.code {
				t.Fatalf("trialOutcome() = %q/%d, want %q/%d", status, code, test.status, test.code)
			}
		})
	}
}
