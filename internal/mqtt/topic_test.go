package mqtt

import "testing"

func TestValidateAndMatch(t *testing.T) {
	validTopics := []string{"a", "a/b/c", "devices/dev-1/telemetry/temp", "a//b"}
	for _, topic := range validTopics {
		if err := ValidateTopic(topic); err != nil {
			t.Errorf("ValidateTopic(%q) unexpected error: %v", topic, err)
		}
	}
	invalidTopics := []string{"", "a/#", "a/+/b", "a+/b"}
	for _, topic := range invalidTopics {
		if err := ValidateTopic(topic); err == nil {
			t.Errorf("ValidateTopic(%q) expected error, got nil", topic)
		}
	}
	validFilters := []string{"#", "+", "a/+/c", "a/#", "devices/+/telemetry/#", "+/b"}
	for _, f := range validFilters {
		if err := ValidateFilter(f); err != nil {
			t.Errorf("ValidateFilter(%q) unexpected error: %v", f, err)
		}
	}
	invalidFilters := []string{"", "a/#/b", "#/a", "a+/b", "a/b#"}
	for _, f := range invalidFilters {
		if err := ValidateFilter(f); err == nil {
			t.Errorf("ValidateFilter(%q) expected error, got nil", f)
		}
	}

	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"a/#", "a", true}, // # 匹配零层
		{"a/#", "a/b/c", true},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/x/c", true},
		{"a/+/c", "a/b/d", false},
		{"#", "anything/here", true},
		{"devices/+/telemetry/#", "devices/dev-1/telemetry/temp/1", true},
		{"devices/+/telemetry/#", "devices/dev-1/cmd/x", false},
		{"a/b", "a/", false},
		{"a/", "a/", true},
	}
	for _, c := range cases {
		got := Match(c.filter, c.topic)
		t.Logf("Match(filter=%q, topic=%q) = %v (want %v)", c.filter, c.topic, got, c.want)
		if got != c.want {
			t.Errorf("Match(%q,%q)=%v want %v", c.filter, c.topic, got, c.want)
		}
	}
}
