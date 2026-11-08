package contractjson

import "testing"

func TestSafeIntegerDomain(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"positive maximum", "9007199254740991", true},
		{"positive overflow", "9007199254740992", false},
		{"negative minimum", "-9007199254740991", true},
		{"negative overflow", "-9007199254740992", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Decode([]byte(`{"value":` + test.value + `}`))
			if test.valid && err != nil {
				t.Fatalf("expected valid integer: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("expected unsafe integer to be rejected")
			}
		})
	}
}

func TestDuplicateObjectKeysAreRejectedAtAnyDepth(t *testing.T) {
	for _, raw := range []string{
		`{"value":1,"value":2}`,
		`{"nested":{"value":1,"value":2}}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatal("expected duplicate object key to be rejected")
		}
	}
}
