package main

import (
	"testing"

	"encoding/base64"
	"github.com/stretchr/testify/require"
)

var (
	dummyString           = []byte("This is a simple string")
	dummyStringB64        = base64.StdEncoding.EncodeToString(dummyString)
	dummyStringNewline    = []byte("This is a simple string newline\n")
	dummyStringNewlineB64 = base64.StdEncoding.EncodeToString(
		dummyStringNewline,
	)
)

// TestSecretToString makes sure that a raw secret can be turned into a string
// correctly.
func TestSecretToString(t *testing.T) {
	testCases := []struct {
		name      string
		input     []byte
		base64    bool
		expectErr bool
		result    string
	}{{
		name:   "plain string",
		input:  dummyString,
		result: string(dummyString),
	}, {
		name:   "plain base64",
		input:  []byte(dummyStringB64),
		base64: true,
		result: string(dummyString),
	}, {
		name:      "invalid base64",
		input:     dummyString,
		base64:    true,
		expectErr: true,
	}, {
		name:   "plain base64 with newline in encoded",
		input:  []byte(dummyStringB64 + "\r\n"),
		base64: true,
		result: string(dummyString),
	}, {
		name:   "string with newline",
		input:  dummyStringNewline,
		result: string(dummyStringNewline),
	}, {
		name:   "base64 with newline in original",
		input:  []byte(dummyStringNewlineB64),
		base64: true,
		result: string(dummyStringNewline),
	}, {
		name:   "base64 with newline in encoded",
		input:  []byte(dummyStringNewlineB64 + "\r\n"),
		base64: true,
		result: string(dummyStringNewline),
	}}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(tt *testing.T) {
			result, err := secretToString(tc.input, tc.base64)

			if tc.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.result, result)
		})
	}
}
