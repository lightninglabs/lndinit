package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/vault/api"
	auth "github.com/hashicorp/vault/api/auth/kubernetes"
)

const (
	// defaultK8sServiceAccountTokenPath is the default location of the
	// projected Kubernetes ServiceAccount token that is used to
	// authenticate against Vault's Kubernetes auth method.
	defaultK8sServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/" +
		"serviceaccount/token"

	// defaultVaultKVMount is the default mount path of the KV v2 secrets
	// engine.
	defaultVaultKVMount = "secret"

	// defaultVaultAuthMount is the default mount path of the Vault
	// Kubernetes auth method.
	defaultVaultAuthMount = "kubernetes"
)

// vaultSecretOptions holds all the flags that are needed to connect to Vault,
// authenticate against it via the Kubernetes auth method and address a single
// entry within a KV v2 secret. It is shared by the store-secret and
// load-secret commands.
type vaultSecretOptions struct {
	Addr          string `long:"addr" description:"The address of the Vault server; if unset, the VAULT_ADDR environment variable is used"`
	AuthMount     string `long:"auth-mount" description:"The mount path of the Vault Kubernetes auth method"`
	AuthRole      string `long:"auth-role" description:"The role to assume when logging into Vault via the Kubernetes auth method"`
	AuthTokenPath string `long:"auth-token-path" description:"The full path to the Kubernetes ServiceAccount token file that is used to authenticate against Vault"`
	KVMount       string `long:"kv-mount" description:"The mount path of the KV v2 secrets engine the secret lives in"`
	SecretPath    string `long:"secret-path" description:"The path of the secret within the KV v2 engine, excluding the engine mount and the 'data/' segment (e.g. 'lnd/mynode/wallet')"`
	SecretKeyName string `long:"secret-key-name" description:"The name of the key/entry within the secret"`
}

// jsonVaultObject is the subset of Vault response metadata that we surface when
// the load-secret command is asked for JSON output.
type jsonVaultObject struct {
	RequestID     string `json:"request_id"`
	LeaseID       string `json:"lease_id"`
	LeaseDuration int    `json:"lease_duration"`
	Renewable     bool   `json:"renewable"`
}

// newJSONVaultObject extracts the response metadata from a KV v2 secret so it
// can be marshalled alongside the secret's value.
func newJSONVaultObject(secret *api.KVSecret) *jsonVaultObject {
	if secret == nil || secret.Raw == nil {
		return &jsonVaultObject{}
	}

	return &jsonVaultObject{
		RequestID:     secret.Raw.RequestID,
		LeaseID:       secret.Raw.LeaseID,
		LeaseDuration: secret.Raw.LeaseDuration,
		Renewable:     secret.Raw.Renewable,
	}
}

// kvMount returns the configured KV v2 mount path, falling back to the default
// if none was set.
func kvMount(opts *vaultSecretOptions) string {
	if opts.KVMount == "" {
		return defaultVaultKVMount
	}

	return opts.KVMount
}

// saveVault stores a single entry within a KV v2 secret. To avoid clobbering
// sibling entries in the same secret, the existing secret (if any) is read
// first and only the addressed key is updated before the merged data is written
// back.
func saveVault(content string, opts *vaultSecretOptions, overwrite bool) error {
	client, err := getClientVault(opts)
	if err != nil {
		return err
	}

	ctx := context.Background()
	kv := client.KVv2(kvMount(opts))

	// Read the current state of the secret so we can merge our entry into
	// it. A not-found error just means we'll be creating the secret fresh.
	data := make(map[string]interface{})
	existing, err := kv.Get(ctx, opts.SecretPath)
	switch {
	case err == nil && existing != nil && existing.Data != nil:
		data = existing.Data

	case err == nil:
		// Secret exists but has no data, we'll initialize it below.

	case errors.Is(err, api.ErrSecretNotFound):
		// Secret doesn't exist yet, we'll create it below.

	default:
		return fmt.Errorf("error querying secret %s in vault: %v",
			opts.SecretPath, err)
	}

	if data[opts.SecretKeyName] != nil && !overwrite {
		return fmt.Errorf("entry %s in secret %s already exists: %v",
			opts.SecretKeyName, opts.SecretPath, errTargetExists)
	}

	data[opts.SecretKeyName] = content

	logger.Infof("Attempting to write entry %s of secret %s in vault",
		opts.SecretKeyName, opts.SecretPath)
	if _, err := kv.Put(ctx, opts.SecretPath, data); err != nil {
		return fmt.Errorf("error writing secret %s in vault: %v",
			opts.SecretPath, err)
	}

	logger.Infof("Stored entry %s of secret %s in vault",
		opts.SecretKeyName, opts.SecretPath)

	return nil
}

// readVault reads a single entry from a KV v2 secret and returns its value
// along with the Vault response metadata.
func readVault(opts *vaultSecretOptions) (string, *jsonVaultObject, error) {
	client, err := getClientVault(opts)
	if err != nil {
		return "", nil, err
	}

	kv := client.KVv2(kvMount(opts))
	secret, err := kv.Get(context.Background(), opts.SecretPath)
	switch {
	case errors.Is(err, api.ErrSecretNotFound):
		return "", nil, fmt.Errorf("secret %s does not exist in vault",
			opts.SecretPath)

	case err != nil:
		return "", nil, fmt.Errorf("error reading secret %s from "+
			"vault: %v", opts.SecretPath, err)
	}

	if secret == nil || len(secret.Data) == 0 {
		return "", nil, fmt.Errorf("secret %s exists but contains no "+
			"data", opts.SecretPath)
	}

	entry, ok := secret.Data[opts.SecretKeyName]
	if !ok || entry == nil {
		return "", nil, fmt.Errorf("secret %s exists but does not "+
			"contain the entry %s", opts.SecretPath,
			opts.SecretKeyName)
	}

	stringEntry, isString := entry.(string)
	if !isString {
		return "", nil, fmt.Errorf("entry %s of secret %s is not a "+
			"string value", opts.SecretKeyName, opts.SecretPath)
	}
	if len(stringEntry) == 0 {
		return "", nil, fmt.Errorf("secret %s exists but the entry %s "+
			"is empty", opts.SecretPath, opts.SecretKeyName)
	}

	// Remove any newlines at the end of the value. We never write a newline
	// ourselves, but the entry may have been provisioned by another process
	// or user.
	content := strings.TrimRight(stringEntry, "\r\n")

	return content, newJSONVaultObject(secret), nil
}

// getClientVault creates a Vault client from the environment and authenticates
// it against the configured Kubernetes auth method.
func getClientVault(opts *vaultSecretOptions) (*api.Client, error) {
	logger.Infof("Creating vault config from environment")
	cfg := api.DefaultConfig()
	if cfg.Error != nil {
		return nil, fmt.Errorf("error reading vault config from env: "+
			"%v", cfg.Error)
	}

	// An explicit address flag overrides whatever the environment provided.
	if opts.Addr != "" {
		cfg.Address = opts.Addr
	}

	logger.Infof("Creating vault client for server %s", cfg.Address)
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating vault client: %v", err)
	}

	if opts.AuthRole == "" {
		return nil, fmt.Errorf("the --vault.auth-role flag must be set")
	}

	authMount := opts.AuthMount
	if authMount == "" {
		authMount = defaultVaultAuthMount
	}

	tokenPath := opts.AuthTokenPath
	if tokenPath == "" {
		tokenPath = defaultK8sServiceAccountTokenPath
	}

	logger.Infof("Authenticating against vault kubernetes auth method %s "+
		"with role %s", authMount, opts.AuthRole)
	k8sAuth, err := auth.NewKubernetesAuth(
		opts.AuthRole,
		auth.WithMountPath(authMount),
		auth.WithServiceAccountTokenPath(tokenPath),
	)
	if err != nil {
		return nil, fmt.Errorf("error setting up kubernetes auth: %v",
			err)
	}

	authInfo, err := client.Auth().Login(context.Background(), k8sAuth)
	if err != nil {
		return nil, fmt.Errorf("error logging into vault: %v", err)
	}
	if authInfo == nil || authInfo.Auth == nil {
		return nil, fmt.Errorf("no authentication info returned after " +
			"vault login")
	}

	logger.Infof("Authenticated successfully with vault, acquired "+
		"policies %v", authInfo.Auth.Policies)

	logger.Infof("Vault client created successfully")
	return client, nil
}
