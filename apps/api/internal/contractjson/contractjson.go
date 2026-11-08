package contractjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"

	"github.com/gowebpki/jcs"
)

const maxSafeInteger = int64(9_007_199_254_740_991)

func Decode(raw []byte) (any, error) {
	value, _, err := validate(raw)
	return value, err
}

func Canonicalize(raw []byte) ([]byte, error) {
	_, canonical, err := validate(raw)
	return canonical, err
}

func validate(raw []byte) (any, []byte, error) {
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("JSON is not a valid I-JSON/JCS input: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, nil, fmt.Errorf("decode JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, nil, errors.New("unexpected data after JSON document")
		}
		return nil, nil, fmt.Errorf("decode trailing JSON data: %w", err)
	}
	if err := validateNumberDomain(value); err != nil {
		return nil, nil, err
	}
	return value, canonical, nil
}

func validateNumberDomain(value any) error {
	max := big.NewInt(maxSafeInteger)
	var visit func(any) error
	visit = func(current any) error {
		switch current := current.(type) {
		case map[string]any:
			for _, nested := range current {
				if err := visit(nested); err != nil {
					return err
				}
			}
		case []any:
			for _, nested := range current {
				if err := visit(nested); err != nil {
					return err
				}
			}
		case json.Number:
			text := current.String()
			if strings.ContainsAny(text, ".eE") {
				return nil
			}
			integer, ok := new(big.Int).SetString(text, 10)
			if !ok {
				return fmt.Errorf("invalid JSON integer %q", text)
			}
			if new(big.Int).Abs(integer).Cmp(max) > 0 {
				return fmt.Errorf("JSON integer %q exceeds the JCS safe integer domain", text)
			}
		}
		return nil
	}
	return visit(value)
}
