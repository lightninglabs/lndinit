package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// newTestVault spins up an httptest server that emulates the subset of the
// Vault HTTP API that lndinit uses: the Kubernetes auth login endpoint and the
// KV v2 data endpoints. It returns the server URL and the backing in-memory
// store keyed by KV path.
func newTestVault(t *testing.T) (string, map[string]map[string]interface{}) {
	t.Helper()

	store := make(map[string]map[string]interface{})

	const dataPrefix = "/v1/secret/data/"

	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Kubernetes auth login: hand back a usable client token.
		if strings.HasSuffix(r.URL.Path, "/auth/kubernetes/login") {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "test-token",
					"policies":       []string{"lnd"},
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
			return
		}

		if !strings.HasPrefix(r.URL.Path, dataPrefix) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, dataPrefix)
		switch r.Method {
		// KV v2 read: nest the values under data.data, as Vault does.
		case http.MethodGet:
			data, ok := store[path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(
					map[string]interface{}{
						"errors": []string{},
					},
				)
				return
			}

			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"request_id": "req-123",
				"data": map[string]interface{}{
					"data": data,
					"metadata": map[string]interface{}{
						"version":      1,
						"created_time": "2020-01-01T00:00:00Z",
					},
				},
			})

		// KV v2 write: the payload wraps the values under a data key.
		case http.MethodPost, http.MethodPut:
			var body struct {
				Data map[string]interface{} `json:"data"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			store[path] = body.Data

			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"version":      1,
					"created_time": "2020-01-01T00:00:00Z",
				},
			})

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(srv.Close)

	return srv.URL, store
}

// testVaultOptions returns a vaultSecretOptions pointed at the test server with
// a temporary ServiceAccount token file.
func testVaultOptions(t *testing.T, addr, path, key string) *vaultSecretOptions {
	t.Helper()

	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("jwt-token"), 0600))

	return &vaultSecretOptions{
		Addr:          addr,
		AuthMount:     defaultVaultAuthMount,
		AuthRole:      "lnd",
		AuthTokenPath: tokenFile,
		KVMount:       defaultVaultKVMount,
		SecretPath:    path,
		SecretKeyName: key,
	}
}

// TestVaultSaveAndRead verifies the round trip of storing and reading a single
// entry, and that adding a second entry does not clobber the first.
func TestVaultSaveAndRead(t *testing.T) {
	addr, _ := newTestVault(t)
	path := "lnd/test/wallet"

	pwOpts := testVaultOptions(t, addr, path, "walletpassword")
	require.NoError(t, saveVault("supersecret", pwOpts, false))

	content, meta, err := readVault(pwOpts)
	require.NoError(t, err)
	require.Equal(t, "supersecret", content)
	require.NotNil(t, meta)

	// Store a second entry in the same secret and make sure the first one
	// survives the merge.
	seedOpts := testVaultOptions(t, addr, path, "walletseed")
	require.NoError(t, saveVault("my sixteen words", seedOpts, false))

	seed, _, err := readVault(seedOpts)
	require.NoError(t, err)
	require.Equal(t, "my sixteen words", seed)

	pw, _, err := readVault(pwOpts)
	require.NoError(t, err)
	require.Equal(t, "supersecret", pw)
}

// TestVaultOverwriteGuard verifies that an existing entry is only replaced when
// the overwrite flag is set, and that the non-overwrite path surfaces the
// errTargetExists sentinel used for lndinit's idempotency contract.
func TestVaultOverwriteGuard(t *testing.T) {
	addr, _ := newTestVault(t)
	opts := testVaultOptions(t, addr, "lnd/test/wallet", "walletpassword")

	require.NoError(t, saveVault("first", opts, false))

	// Storing again without overwrite must fail with the sentinel.
	err := saveVault("second", opts, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), errTargetExists)

	// The original value must be untouched.
	content, _, err := readVault(opts)
	require.NoError(t, err)
	require.Equal(t, "first", content)

	// With overwrite, the value is replaced.
	require.NoError(t, saveVault("second", opts, true))
	content, _, err = readVault(opts)
	require.NoError(t, err)
	require.Equal(t, "second", content)
}

// TestVaultReadTrimsNewline makes sure trailing newlines on a stored value are
// stripped on read.
func TestVaultReadTrimsNewline(t *testing.T) {
	addr, _ := newTestVault(t)
	opts := testVaultOptions(t, addr, "lnd/test/wallet", "walletpassword")

	require.NoError(t, saveVault("value-with-newline\r\n", opts, false))

	content, _, err := readVault(opts)
	require.NoError(t, err)
	require.Equal(t, "value-with-newline", content)
}

// TestVaultReadErrors covers the not-found, missing-entry and empty-entry
// error paths.
func TestVaultReadErrors(t *testing.T) {
	addr, _ := newTestVault(t)

	// Secret does not exist at all.
	missing := testVaultOptions(t, addr, "lnd/missing/wallet", "walletpassword")
	_, _, err := readVault(missing)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")

	// Secret exists but the requested entry does not.
	present := testVaultOptions(t, addr, "lnd/test/wallet", "walletpassword")
	require.NoError(t, saveVault("supersecret", present, false))

	wrongKey := testVaultOptions(t, addr, "lnd/test/wallet", "does-not-exist")
	_, _, err = readVault(wrongKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not contain the entry")

	// Entry exists but is empty.
	empty := testVaultOptions(t, addr, "lnd/test/wallet", "empty")
	require.NoError(t, saveVault("", empty, false))
	_, _, err = readVault(empty)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

// TestStoreSecretsVault exercises the batch store helper used by the
// store-secret command.
func TestStoreSecretsVault(t *testing.T) {
	addr, _ := newTestVault(t)
	opts := testVaultOptions(t, addr, "lnd/test/rpc", "")

	entries := []*entry{
		{key: "tls.cert", value: "cert-bytes"},
		{key: "admin.macaroon", value: "macaroon-bytes"},
	}
	require.NoError(t, storeSecretsVault(entries, opts, false))

	for _, e := range entries {
		readOpts := testVaultOptions(t, addr, "lnd/test/rpc", e.key)
		content, _, err := readVault(readOpts)
		require.NoError(t, err)
		require.Equal(t, e.value, content)
	}

	// A missing secret path must be rejected.
	badOpts := testVaultOptions(t, addr, "", "tls.cert")
	require.Error(t, storeSecretsVault(entries, badOpts, false))
}
