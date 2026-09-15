package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/jessevdk/go-flags"
	api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core "k8s.io/client-go/kubernetes/typed/core/v1"
)

// storeAccountBackupCommand exports LND's synchronous recovery file to a
// secondary Kubernetes copy. It never touches seed or RPC credential entries.
type storeAccountBackupCommand struct {
	File      string        `long:"file" description:"LND public wallet-account-backup file" required:"true"`
	Namespace string        `long:"namespace" description:"Recovery Secret namespace" required:"true"`
	Name      string        `long:"secret-name" description:"Dedicated recovery Secret name" required:"true"`
	Timeout   time.Duration `long:"timeout" default:"10s" description:"Maximum Kubernetes write duration"`
}

// newStoreAccountBackupCommand creates the metadata-only export command.
func newStoreAccountBackupCommand() *storeAccountBackupCommand { return &storeAccountBackupCommand{} }

// Register exposes an atomic metadata export, separate from store-secret's
// generic overwrite behavior and legacy credential encoding.
func (x *storeAccountBackupCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"store-account-backup", "Export public account recovery "+
			"metadata", "Copy LND's synchronous account backup "+
			"to Kubernetes with identity checks and monotonic "+
			"branch counts. This secondary copy does not "+
			"replace the independent synchronous file.", x,
	)

	return err
}

// Execute reads only the public file and bounds all API retries. Payloads and
// API error bodies are intentionally absent from errors and logs.
func (x *storeAccountBackupCommand) Execute(_ []string) error {
	if x.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	data, err := os.ReadFile(x.File)
	if err != nil {
		return errors.New("read account backup file failed")
	}
	if _, err = mergeAccountExports(nil, data); err != nil {
		return err
	}
	client, err := getClientK8s()
	if err != nil {
		return errors.New("create Kubernetes client failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), x.Timeout)
	defer cancel()

	return exportAccountBackup(
		ctx, client.CoreV1().Secrets(x.Namespace), x.Name, data,
	)
}

// exportAccountBackup uses resourceVersion compare-and-swap, rereading and
// merging after conflicts. All accounts commit together; unrelated data stays.
func exportAccountBackup(ctx context.Context, secrets core.SecretInterface,
	name string, data []byte) error {

	for attempt := 0; attempt < 8; attempt++ {
		current, err := secrets.Get(ctx, name, metav1.GetOptions{})
		create := apierrors.IsNotFound(err)
		if err != nil && !create {
			return errors.New("read account recovery Secret failed")
		}
		if create {
			current = &api.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
				},
				Type: api.SecretTypeOpaque,
			}
		}
		merged, err := mergeAccountExports(
			current.Data["accounts.json"], data,
		)
		if err != nil {
			return err
		}
		if current.Data == nil {
			current.Data = make(map[string][]byte)
		}
		current.Data["accounts.json"] = merged
		if create {
			_, err = secrets.Create(
				ctx, current, metav1.CreateOptions{},
			)
		} else {
			_, err = secrets.Update(
				ctx, current, metav1.UpdateOptions{},
			)
		}
		if err == nil {
			return nil
		}
		if !apierrors.IsConflict(err) &&
			!apierrors.IsAlreadyExists(err) {
			return errors.New("write account recovery Secret " +
				"failed")
		}
	}

	return errors.New("account recovery Secret update conflicts exhausted")
}

// accountExport retains every identity field as raw JSON so an export cannot
// silently discard recovery fields introduced by LND.
type accountExport struct {
	Version   uint32                       `json:"version"`
	Network   string                       `json:"network"`
	UpdatedAt time.Time                    `json:"updated_at"`
	Accounts  []map[string]json.RawMessage `json:"accounts"`
}

// parseAccountExport requires explicit branch bounds and unique name/scope
// identities. Unsupported format versions fail instead of losing new fields.
func parseAccountExport(data []byte) (*accountExport, error) {
	var s accountExport
	if json.Unmarshal(data, &s) != nil || s.Version != 1 ||
		s.Network == "" || len(s.Accounts) == 0 {
		return nil, errors.New("invalid account backup format")
	}
	seen := make(map[string]bool)
	for _, a := range s.Accounts {
		// Compare values independent of JSON escaping and whitespace.
		// UseNumber preserves exact values in additional identity fields.
		for field, raw := range a {
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			var value any
			if decoder.Decode(&value) != nil {
				return nil, errors.New("invalid account field")
			}
			a[field], _ = json.Marshal(value)
		}
		for _, field := range []string{
			"name",
			"purpose",
			"coin",
			"account_index",
			"external_address_type",
			"internal_address_type",
			"extended_public_key",
			"master_key_fingerprint",
			"watch_only",
			"external_key_count",
			"internal_key_count",
		} {
			if len(a[field]) == 0 || string(a[field]) == "null" {
				return nil, errors.New("incomplete account " +
					"recovery record")
			}
		}
		var external, internal uint32
		if json.Unmarshal(a["external_key_count"], &external) != nil ||
			json.Unmarshal(a["internal_key_count"], &internal) != nil {
			return nil, errors.New("invalid account branch count")
		}
		key := exportAccountKey(a)
		if seen[key] {
			return nil, errors.New("duplicate account recovery " +
				"identity")
		}
		seen[key] = true
	}

	return &s, nil
}

// exportAccountKey matches LND's scope/name identity, including imported keys
// whose foreign derivation index can equal the local default account's index.
func exportAccountKey(a map[string]json.RawMessage) string {
	return string(a["purpose"]) + "/" + string(a["coin"]) + "/" + string(
		a["name"],
	)
}

// mergeAccountExports preserves a union and maxima without granting freshness
// to stale input. A changed identity fails the whole transaction.
func mergeAccountExports(oldData, liveData []byte) ([]byte, error) {
	live, err := parseAccountExport(liveData)
	if err != nil {
		return nil, err
	}
	if len(oldData) == 0 {
		return json.Marshal(live)
	}
	old, err := parseAccountExport(oldData)
	if err != nil {
		return nil, err
	}
	if old.Network != live.Network {
		return nil, errors.New("account backup network mismatch")
	}
	positions := make(map[string]int)
	for i, a := range live.Accounts {
		positions[exportAccountKey(a)] = i
	}
	for _, previous := range old.Accounts {
		i, ok := positions[exportAccountKey(previous)]
		if !ok {
			live.Accounts = append(live.Accounts, previous)
			continue
		}
		current := live.Accounts[i]
		for field, value := range previous {
			if field == "external_key_count" ||
				field == "internal_key_count" {

				var before, after uint32
				_ = json.Unmarshal(value, &before)
				_ = json.Unmarshal(current[field], &after)
				current[field], _ = json.Marshal(
					max(before, after),
				)
			} else if string(value) != string(current[field]) {
				return nil, errors.New("account backup " +
					"identity mismatch")
			}
		}
	}
	if old.UpdatedAt.After(live.UpdatedAt) {
		live.UpdatedAt = old.UpdatedAt
	}

	return json.Marshal(live)
}
