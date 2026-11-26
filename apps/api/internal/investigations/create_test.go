package investigations

import (
	"testing"
)

func TestNormalizeSubject(t *testing.T) {
	name := "  Ada   LOVELACE "
	normalized, err := NormalizeSubject(Subject{
		Type: "person",
		Name: &name,
		Identifiers: map[string]string{
			" Registration Number ": " 42 ",
		},
	})
	if err != nil {
		t.Fatalf("NormalizeSubject() error = %v", err)
	}
	if normalized.Name == nil || *normalized.Name != "ada lovelace" {
		t.Fatalf("normalized name = %v, want ada lovelace", normalized.Name)
	}
	if normalized.Identifiers["registration_number"] != "42" {
		t.Fatalf("normalized identifiers = %#v", normalized.Identifiers)
	}
}

func TestNormalizeSubjectRejectsInvalidInput(t *testing.T) {
	empty := " "
	tests := []Subject{
		{Type: "company", Name: &empty},
		{Type: "person", Name: &empty},
		{Type: "person", Identifiers: map[string]string{"": "42"}},
		{Type: "organization", Identifiers: map[string]string{"id": " "}},
	}
	for _, subject := range tests {
		if _, err := NormalizeSubject(subject); err == nil {
			t.Errorf("NormalizeSubject(%#v) error = nil, want validation error", subject)
		}
	}
}
