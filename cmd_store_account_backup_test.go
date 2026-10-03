package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// exportFixture supplies public synthetic metadata in the LND file format.
func exportFixture(external, internal int) []byte {
	return []byte(
		fmt.Sprintf(`{"version":1,"network":"regtest","updated_at":"2026-09-10T00:00:00Z","accounts":[{"name":"treasury","purpose":86,"coin":0,"account_index":2,"external_address_type":4,"internal_address_type":4,"extended_public_key":"public-test-key","master_key_fingerprint":0,"watch_only":false,"external_key_count":%d,"internal_key_count":%d}]}`,
			external, internal),
	)
}

// TestAccountExportCAS verifies retry uses fresh metadata and preserves every
// unrelated Secret key after a concurrent newer writer wins the first update.
func TestAccountExportCAS(t *testing.T) {
	t.Parallel()
	original := &api.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "recovery",
			Namespace:       "test",
			ResourceVersion: "1",
		},
		Data: map[string][]byte{
			"accounts.json": exportFixture(1, 1),
			"unrelated":     []byte("synthetic"),
		},
	}
	client := fake.NewSimpleClientset(original)
	updates := 0
	client.PrependReactor(
		"update", "secrets",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			updates++
			// The fake tracker does not enforce resourceVersion.
			update, ok := action.(ktesting.UpdateAction)
			require.True(t, ok)
			candidate, ok := update.GetObject().(*api.Secret)
			require.True(t, ok)
			require.Equal(t, fmt.Sprint(updates),
				candidate.ResourceVersion)
			if updates != 1 {
				return false, nil, nil
			}
			newer := original.DeepCopy()
			newer.ResourceVersion = "2"
			newer.Data["accounts.json"] = exportFixture(9, 11)
			require.NoError(
				t,
				client.Tracker().Update(api.SchemeGroupVersion.WithResource(
					"secrets",
				),
					newer, "test",
				),
			)

			return true, nil, apierrors.NewConflict(
				schema.GroupResource{
					Resource: "secrets",
				}, "recovery",
				errors.New("concurrent update"),
			)
		},
	)
	require.NoError(
		t,
		exportAccountBackup(
			context.Background(), client.CoreV1().Secrets("test"),
			"recovery", exportFixture(7, 3),
		),
	)
	result, err := client.CoreV1().Secrets("test").Get(
		context.Background(),
		"recovery", metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, []byte("synthetic"), result.Data["unrelated"])
	decoded, err := parseAccountExport(result.Data["accounts.json"])
	require.NoError(t, err)
	require.JSONEq(
		t, "9", string(decoded.Accounts[0]["external_key_count"]),
	)
	require.JSONEq(
		t, "11", string(decoded.Accounts[0]["internal_key_count"]),
	)
	require.Equal(t, 2, updates)
}

// TestAccountExportFailures preserves the complete previous object on failed
// writes, identity mismatch, incomplete branch metadata, and RPC-style errors.
func TestAccountExportFailures(t *testing.T) {
	t.Parallel()
	original := exportFixture(2, 8)
	client := fake.NewSimpleClientset(
		&api.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "recovery",
				Namespace: "test",
			},
			Data: map[string][]byte{
				"accounts.json": original,
			},
		},
	)
	client.PrependReactor(
		"update", "secrets",
		func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("transient API failure")
		},
	)
	require.Error(
		t,
		exportAccountBackup(
			context.Background(), client.CoreV1().Secrets("test"),
			"recovery", exportFixture(3, 9),
		),
	)
	result, err := client.CoreV1().Secrets("test").Get(
		context.Background(),
		"recovery", metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, original, result.Data["accounts.json"])
	bad, err := parseAccountExport(original)
	require.NoError(t, err)
	delete(bad.Accounts[0], "internal_key_count")
	encoded, err := json.Marshal(bad)
	require.NoError(t, err)
	_, err = mergeAccountExports(original, encoded)
	require.ErrorContains(t, err, "incomplete")
	bad, err = parseAccountExport(original)
	require.NoError(t, err)
	bad.Accounts[0]["extended_public_key"] = json.RawMessage(
		`"changed-key"`,
	)
	encoded, err = json.Marshal(bad)
	require.NoError(t, err)
	_, err = mergeAccountExports(original, encoded)
	require.ErrorContains(t, err, "identity mismatch")
}

// TestAccountExportCreate supports first export without changing the RPC
// Secret, then permits replay and stale export after a container restart.
func TestAccountExportCreate(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset()
	secrets := client.CoreV1().Secrets("test")
	require.NoError(
		t,
		exportAccountBackup(
			context.Background(), secrets, "recovery",
			exportFixture(4, 8),
		),
	)
	require.NoError(
		t,
		exportAccountBackup(
			context.Background(), secrets, "recovery",
			exportFixture(1, 0),
		),
	)
	result, err := secrets.Get(
		context.Background(),
		"recovery", metav1.GetOptions{},
	)
	require.NoError(t, err)
	decoded, err := parseAccountExport(result.Data["accounts.json"])
	require.NoError(t, err)
	require.Equal(t, "8", string(decoded.Accounts[0]["internal_key_count"]))
	require.Len(t, result.Data, 1)
}

// TestAccountExportEquivalentJSON preserves a single identity and its maxima
// across escaping variants, replay, and an export missing a recorded account.
func TestAccountExportEquivalentJSON(t *testing.T) {
	t.Parallel()
	live := bytes.ReplaceAll(exportFixture(4, 8), []byte("treasury"),
		[]byte("a&<b>"))
	stored, err := mergeAccountExports(nil, live)
	require.NoError(t, err)
	for range 3 {
		stored, err = mergeAccountExports(stored, live)
		require.NoError(t, err)
		decoded, err := parseAccountExport(stored)
		require.NoError(t, err)
		require.Len(t, decoded.Accounts, 1)
		require.Equal(t, "8", string(
			decoded.Accounts[0]["internal_key_count"],
		))
	}
	stored, err = mergeAccountExports(stored, exportFixture(1, 0))
	require.NoError(t, err)
	decoded, err := parseAccountExport(stored)
	require.NoError(t, err)
	require.Len(t, decoded.Accounts, 2)
	require.Equal(t, "8", string(
		decoded.Accounts[1]["internal_key_count"],
	))
}
