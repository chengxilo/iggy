package tests

import (
	"testing"

	"github.com/cucumber/godog"
)

func TestFeatures(t *testing.T) {
	suites := []godog.TestSuite{{
		ScenarioInitializer: initBasicMessagingScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"../../scenarios/basic_messaging.feature"},
			TestingT: t,
		},
	}, {
		ScenarioInitializer: initLeaderRedirectionScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"../../scenarios/leader_redirection.feature"},
			TestingT: t,
		},
	}}
	for _, s := range suites {
		if s.Run() != 0 {
			t.Fatal("failing feature tests")
		}
	}
}
