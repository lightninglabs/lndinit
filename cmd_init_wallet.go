package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcwallet/wallet"
	"github.com/jessevdk/go-flags"
	"github.com/lightninglabs/protobuf-hex-display/jsonpb" // nolint
	"github.com/lightningnetwork/lnd/aezeed"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/walletrpc"
	"github.com/lightningnetwork/lnd/signal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	defaultNoFreelistSync = true

	defaultDirPermissions os.FileMode = 0700

	defaultBitcoinNetwork = "mainnet"

	typeFile = "file"
	typeRpc  = "rpc"

	// birthdayDateFormat is the calendar date notation a watch-only
	// wallet's birthday can be expressed in, next to a raw Unix timestamp
	// and a full RFC3339 timestamp.
	birthdayDateFormat = "2006-01-02"

	// maxBirthdayDrift is how far into the future a watch-only wallet's
	// birthday is allowed to be. We tolerate some slack to account for
	// clock skew between the machine that produced the timestamp and the
	// one running lndinit. Anything beyond that is rejected, since a
	// birthday far enough in the future makes lnd start scanning at the
	// chain tip, where it would miss any funds the wallet already holds.
	//
	// The window is a day rather than something tighter because btcwallet
	// rewinds the birthday by 48 hours before it looks for the birthday
	// block, so a value inside this window still starts the scan at least a
	// day before the wallet could have been created.
	maxBirthdayDrift = 24 * time.Hour
)

var (
	defaultWalletDBTimeout = 2 * time.Second
)

type secretSourceFile struct {
	Seed           string `long:"seed" description:"The full path to the file that contains the seed; if the file does not exist, lndinit will exit with code EXIT_CODE_INPUT_MISSING (129)"`
	SeedPassphrase string `long:"seed-passphrase" description:"The full path to the file that contains the seed passphrase; if not set, no passphrase will be used; if set but the file does not exist, lndinit will exit with code EXIT_CODE_INPUT_MISSING (129)"`
	WalletPassword string `long:"wallet-password" description:"The full path to the file that contains the wallet password; if the file does not exist, lndinit will exit with code EXIT_CODE_INPUT_MISSING (129)"`
}

type secretSourceK8s struct {
	Namespace             string `long:"namespace" description:"The Kubernetes namespace the secret is located in"`
	SecretName            string `long:"secret-name" description:"The name of the Kubernetes secret"`
	SeedKeyName           string `long:"seed-key-name" description:"The name of the key within the secret that contains the seed"`
	SeedPassphraseKeyName string `long:"seed-passphrase-key-name" description:"The name of the key within the secret that contains the seed passphrase"`
	WalletPasswordKeyName string `long:"wallet-password-key-name" description:"The name of the key within the secret that contains the wallet password"`
	Base64                bool   `long:"base64" description:"Encode as base64 when storing and decode as base64 when reading"`
}

type initTypeFile struct {
	OutputWalletDir  string `long:"output-wallet-dir" description:"The directory in which the wallet.db file should be initialized"`
	ValidatePassword bool   `long:"validate-password" description:"If a wallet file already exists in the output wallet directory, validate that it can be unlocked with the given password; this will try to decrypt the wallet and will take several seconds to complete"`
}

type initTypeRpc struct {
	Server            string `long:"server" description:"The host:port of the RPC server to connect to"`
	TLSCertPath       string `long:"tls-cert-path" description:"The full path to the RPC server's TLS certificate"`
	WatchOnly         bool   `long:"watch-only" description:"Don't require a seed to be set, initialize the wallet as watch-only; requires the accounts-file flag to be specified"`
	AccountsFile      string `long:"accounts-file" description:"The JSON file that contains all accounts xpubs for initializing a watch-only wallet"`
	WatchOnlyBirthday string `long:"watch-only-birthday" description:"The birthday of the watch-only wallet's master key, either as a Unix timestamp in seconds, an RFC3339 timestamp or a YYYY-MM-DD date; if unset, lnd assumes the aezeed epoch (2017-08-24) and rescans the chain from there, which can take hours; requires the watch-only flag to be specified"`
}

type initWalletCommand struct {
	Network      string            `long:"network" description:"The Bitcoin network to initialize the wallet for, required for wallet internals" choice:"mainnet" choice:"testnet" choice:"testnet3" choice:"regtest" choice:"simnet"`
	SecretSource string            `long:"secret-source" description:"Where to read the secrets from to initialize the wallet with" choice:"file" choice:"k8s"`
	File         *secretSourceFile `group:"Flags for reading the secrets from files (use when --secret-source=file)" namespace:"file"`
	K8s          *secretSourceK8s  `group:"Flags for reading the secrets from Kubernetes (use when --secret-source=k8s)" namespace:"k8s"`
	InitType     string            `long:"init-type" description:"How to initialize the wallet" choice:"file" choice:"rpc"`
	InitFile     *initTypeFile     `group:"Flags for initializing the wallet as a file (use when --init-type=file)" namespace:"init-file"`
	InitRpc      *initTypeRpc      `group:"Flags for initializing the wallet through RPC (use when --init-type=rpc)" namespace:"init-rpc"`
}

func newInitWalletCommand() *initWalletCommand {
	return &initWalletCommand{
		Network:      defaultBitcoinNetwork,
		SecretSource: storageFile,
		File:         &secretSourceFile{},
		K8s: &secretSourceK8s{
			Namespace: defaultK8sNamespace,
		},
		InitType: typeFile,
		InitFile: &initTypeFile{},
		InitRpc: &initTypeRpc{
			Server: defaultRPCServer,
		},
	}
}

func (x *initWalletCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"init-wallet",
		"Initialize an lnd wallet database",
		"Create an lnd wallet.db database file initialized with the "+
			"given wallet seed and password",
		x,
	)
	return err
}

func (x *initWalletCommand) Execute(_ []string) error {
	// A birthday can only be given for a watch-only wallet created through
	// RPC. Any other wallet is created from a seed that carries its birthday
	// with it, so silently ignoring the flag there would be misleading.
	birthdayApplies := x.InitType == typeRpc && x.InitRpc.WatchOnly
	if x.InitRpc.WatchOnlyBirthday != "" && !birthdayApplies {
		return fmt.Errorf("--init-rpc.watch-only-birthday can only " +
			"be used in combination with --init-type=rpc and " +
			"--init-rpc.watch-only")
	}

	// Do we require a seed? We don't if we do an RPC based, watch-only
	// initialization.
	requireSeed := (x.InitType == typeFile) ||
		(x.InitType == typeRpc && !x.InitRpc.WatchOnly)

	seed, seedPassPhrase, walletPassword, err := x.readInput(requireSeed)
	if err != nil {
		return fmt.Errorf("error reading input parameters: %v", err)
	}

	switch x.InitType {
	case typeFile:
		cipherSeed, err := checkSeed(seed, seedPassPhrase)
		if err != nil {
			return err
		}

		// The output directory must be specified explicitly. We don't
		// want to assume any defaults here!
		walletDir := lncfg.CleanAndExpandPath(
			x.InitFile.OutputWalletDir,
		)
		if walletDir == "" {
			return fmt.Errorf("must specify output wallet " +
				"directory")
		}
		if strings.HasSuffix(walletDir, ".db") {
			return fmt.Errorf("output wallet directory must not " +
				"be a file")
		}

		return createWalletFile(
			cipherSeed, walletPassword, walletDir, x.Network,
			x.InitFile.ValidatePassword,
		)

	case typeRpc:
		var (
			seedWords []string
			watchOnly *lnrpc.WatchOnly
		)

		if requireSeed {
			_, err = checkSeed(seed, seedPassPhrase)
			if err != nil {
				return err
			}
			seedWords = strings.Split(seed, " ")
		}

		// Only when initializing the wallet through RPC is it possible
		// to create a watch-only wallet. If we do, we don't require a
		// seed to be present but instead want to read an accounts JSON
		// file that contains all the wallet's xpubs.
		if x.InitRpc.WatchOnly {
			// The accounts JSON file doesn't carry the birthday of
			// the master key the accounts were derived from, so the
			// operator has to tell us what it is. Without it lnd
			// rescans the chain from the aezeed epoch, which on
			// mainnet means walking hundreds of thousands of
			// blocks.
			birthday, err := parseWatchOnlyBirthday(
				x.InitRpc.WatchOnlyBirthday, x.Network,
			)
			if err != nil {
				return err
			}

			if birthday == 0 {
				logger.Warn("No wallet birthday specified, " +
					"lnd will rescan the chain from the " +
					"aezeed epoch (2017-08-24) which can " +
					"take multiple hours; use " +
					"--init-rpc.watch-only-birthday to " +
					"start the rescan at the wallet's " +
					"actual birthday instead")
			} else {
				logger.Infof("Using wallet birthday %s",
					formatBirthday(birthday))
			}

			// For initializing a watch-only wallet we need the
			// accounts JSON file.
			logger.Info("Reading accounts from file")
			accountsBytes, err := readFile(x.InitRpc.AccountsFile)
			if err != nil {
				return err
			}

			jsonAccts := &walletrpc.ListAccountsResponse{}
			err = jsonpb.Unmarshal(
				strings.NewReader(accountsBytes), jsonAccts,
			)
			if err != nil {
				return fmt.Errorf("error parsing JSON: %v", err)
			}
			if len(jsonAccts.Accounts) == 0 {
				return fmt.Errorf("cannot import empty " +
					"account list")
			}

			rpcAccounts, err := walletrpc.AccountsToWatchOnly(
				jsonAccts.Accounts,
			)
			if err != nil {
				return fmt.Errorf("error converting JSON "+
					"accounts to RPC: %v", err)
			}

			watchOnly = &lnrpc.WatchOnly{
				MasterKeyBirthdayTimestamp: birthday,
				Accounts:                   rpcAccounts,
			}
		}

		return createWalletRpc(
			seedWords, seedPassPhrase, walletPassword,
			x.InitRpc.Server, x.InitRpc.TLSCertPath, watchOnly,
		)

	default:
		return fmt.Errorf("invalid init type %s", x.InitType)
	}
}

func (x *initWalletCommand) readInput(requireSeed bool) (string, string, string,
	error) {

	// First find out where we want to read the secrets from.
	var (
		seed           string
		seedPassPhrase string
		walletPassword string
		err            error
	)
	switch x.SecretSource {
	// Read all secrets from individual files.
	case storageFile:
		if requireSeed {
			logger.Info("Reading seed from file")
			seed, err = readFile(x.File.Seed)
			if err != nil {
				return "", "", "", err
			}
		}

		// The seed passphrase is optional.
		if x.File.SeedPassphrase != "" {
			logger.Info("Reading seed passphrase from file")
			seedPassPhrase, err = readFile(x.File.SeedPassphrase)
			if err != nil {
				return "", "", "", err
			}
		}

		logger.Info("Reading wallet password from file")
		walletPassword, err = readFile(x.File.WalletPassword)
		if err != nil {
			return "", "", "", err
		}

	// Read passphrase from Kubernetes secret.
	case storageK8s:
		k8sSecret := &k8sObjectOptions{
			Namespace:  x.K8s.Namespace,
			Name:       x.K8s.SecretName,
			KeyName:    x.K8s.SeedKeyName,
			Base64:     x.K8s.Base64,
			ObjectType: ObjectTypeSecret,
		}

		if requireSeed {
			logger.Infof("Reading seed from k8s secret %s (namespace %s)",
				x.K8s.SecretName, x.K8s.Namespace)
			seed, _, err = readK8s(k8sSecret)
			if err != nil {
				return "", "", "", err
			}
		}

		// The seed passphrase is optional.
		if x.K8s.SeedPassphraseKeyName != "" {
			logger.Infof("Reading seed passphrase from k8s secret %s "+
				"(namespace %s)", x.K8s.SecretName,
				x.K8s.Namespace)
			k8sSecret.KeyName = x.K8s.SeedPassphraseKeyName
			seedPassPhrase, _, err = readK8s(k8sSecret)
			if err != nil {
				return "", "", "", err
			}
		}

		logger.Infof("Reading wallet password from k8s secret %s (namespace %s)",
			x.K8s.SecretName, x.K8s.Namespace)
		k8sSecret.KeyName = x.K8s.WalletPasswordKeyName
		walletPassword, _, err = readK8s(k8sSecret)
		if err != nil {
			return "", "", "", err
		}
	}

	// The seed, its passphrase and the wallet password should all never
	// have a newline at their end, otherwise that might lead to errors
	// further down the line.
	seed = stripNewline(seed)
	seedPassPhrase = stripNewline(seedPassPhrase)
	walletPassword = stripNewline(walletPassword)

	return seed, seedPassPhrase, walletPassword, nil
}

func createWalletFile(cipherSeed *aezeed.CipherSeed, walletPassword, walletDir,
	network string, validatePassword bool) error {

	// The wallet directory must either not exist yet or be a directory.
	stat, err := os.Stat(walletDir)
	switch {
	case os.IsNotExist(err):
		err = os.MkdirAll(walletDir, defaultDirPermissions)
		if err != nil {
			return fmt.Errorf("error creating directory %s: %v",
				walletDir, err)
		}

	case !stat.IsDir():
		return fmt.Errorf("output wallet directory must not be a file")
	}

	// We should now be able to properly determine if a wallet already
	// exists or not. Depending on the flags, we either create or validate
	// the wallet now.
	walletFile := filepath.Join(walletDir, wallet.WalletDBName)
	switch {
	case lnrpc.FileExists(walletFile) && !validatePassword:
		return fmt.Errorf("wallet file %s exists: %v", walletFile,
			errTargetExists)

	case !lnrpc.FileExists(walletFile):
		return createWallet(
			walletDir, cipherSeed, []byte(walletPassword),
			network,
		)

	default:
		return validateWallet(
			walletDir, []byte(walletPassword), network,
		)
	}
}

func createWallet(walletDir string, cipherSeed *aezeed.CipherSeed,
	walletPassword []byte, network string) error {

	logger.Infof("Creating new wallet in %s", walletDir)

	// The network parameters are needed for some wallet internal things
	// like the chain genesis hash and timestamp.
	netParams, err := getNetworkParams(network)
	if err != nil {
		return err
	}

	// Create the wallet now.
	loader := wallet.NewLoader(
		netParams, walletDir, defaultNoFreelistSync,
		defaultWalletDBTimeout, 0,
	)

	_, err = loader.CreateNewWallet(
		walletPassword, walletPassword, cipherSeed.Entropy[:],
		cipherSeed.BirthdayTime(),
	)
	if err != nil {
		return fmt.Errorf("error creating wallet from seed: %v", err)
	}

	// Close the wallet properly to release the file lock on the DB.
	if err := loader.UnloadWallet(); err != nil {
		return fmt.Errorf("error unloading wallet after creation: %v",
			err)
	}

	logger.Infof("Wallet created successfully in %s", walletDir)

	return nil
}

func validateWallet(walletDir string, walletPassword []byte,
	network string) error {

	logger.Infof("Validating password for wallet in %s", walletDir)

	// The network parameters are needed for some wallet internal things
	// like the chain genesis hash and timestamp.
	netParams, err := getNetworkParams(network)
	if err != nil {
		return err
	}

	// Try to load the wallet now. This will fail if the wallet is already
	// loaded by another process or does not exist yet.
	loader := wallet.NewLoader(
		netParams, walletDir, defaultNoFreelistSync,
		defaultWalletDBTimeout, 0,
	)
	_, err = loader.OpenExistingWallet(walletPassword, false)
	if err != nil {
		return fmt.Errorf("error validating wallet password: %v", err)
	}

	if err := loader.UnloadWallet(); err != nil {
		return fmt.Errorf("error unloading wallet after validation: %v",
			err)
	}

	logger.Info("Wallet password validated successfully")

	return nil
}

func createWalletRpc(seedWords []string, seedPassword, walletPassword,
	rpcServer, tlsPath string, watchOnly *lnrpc.WatchOnly) error {

	// Since this will potentially run for a while (we need to wait for
	// compaction), make sure we catch any interrupt signals.
	shutdown, err := signal.Intercept()
	if err != nil {
		return fmt.Errorf("error intercepting signals: %v", err)
	}

	// First, we want to make sure the wallet doesn't actually exist. We
	// wait until we either get the NON_EXISTING code or an error because
	// the desired state wasn't achieved (a state _greater_ than
	// NON_EXISTING was returned, which means the wallet exists).
	timeout := time.Duration(math.MaxInt64)
	err = waitUntilStatus(
		rpcServer, lnrpc.WalletState_NON_EXISTING,
		timeout, shutdown.ShutdownChannel(),
	)
	if err != nil {
		return fmt.Errorf("error waiting for lnd startup: %v", err)
	}

	// We are now certain that the wallet doesn't exist yet, so we can go
	// ahead and try to create it.
	client, err := getUnlockerConnection(rpcServer, tlsPath)
	if err != nil {
		return fmt.Errorf("error creating wallet unlocker connection: "+
			"%v", err)
	}

	ctxb := context.Background()
	_, err = client.InitWallet(ctxb, &lnrpc.InitWalletRequest{
		CipherSeedMnemonic: seedWords,
		AezeedPassphrase:   []byte(seedPassword),
		WalletPassword:     []byte(walletPassword),
		WatchOnly:          watchOnly,
	})
	return err
}

func checkSeed(seed, seedPassPhrase string) (*aezeed.CipherSeed, error) {
	// Decrypt the seed now to make sure we got valid data before we
	// check anything else.
	seedWords := strings.Split(seed, " ")
	if len(seedWords) != aezeed.NumMnemonicWords {
		return nil, fmt.Errorf("invalid seed, expected %d words but "+
			"got %d", aezeed.NumMnemonicWords, len(seedWords))
	}
	var seedMnemonic aezeed.Mnemonic
	copy(seedMnemonic[:], seedWords)
	cipherSeed, err := seedMnemonic.ToCipherSeed([]byte(seedPassPhrase))
	if err != nil {
		return nil, fmt.Errorf("error decrypting seed with "+
			"passphrase: %v", err)
	}

	return cipherSeed, nil
}

// parseWatchOnlyBirthday turns the operator provided birthday of a watch-only
// wallet's master key into a Unix timestamp in seconds, as expected by lnd's
// InitWallet RPC. An empty value means the birthday is unknown and results in a
// zero timestamp, which makes lnd fall back to its own default.
func parseWatchOnlyBirthday(birthday, network string) (uint64, error) {
	if birthday == "" {
		return 0, nil
	}

	birthdayTime, err := parseTimestampOrDate(birthday)
	if err != nil {
		return 0, fmt.Errorf("error parsing wallet birthday: %v", err)
	}

	// A birthday from before the chain itself existed can only be a
	// mistake, and would make lnd rescan all the way from the genesis
	// block.
	netParams, err := getNetworkParams(network)
	if err != nil {
		return 0, err
	}
	genesisTime := netParams.GenesisBlock.Header.Timestamp
	if birthdayTime.Before(genesisTime) {
		return 0, fmt.Errorf("invalid wallet birthday %s, is before "+
			"the %s genesis block time %s",
			birthdayTime.Format(time.RFC3339), network,
			genesisTime.UTC().Format(time.RFC3339))
	}

	if birthdayTime.After(time.Now().Add(maxBirthdayDrift)) {
		return 0, fmt.Errorf("invalid wallet birthday %s, is more "+
			"than %v in the future (are the units seconds?)",
			birthdayTime.Format(time.RFC3339), maxBirthdayDrift)
	}

	return uint64(birthdayTime.Unix()), nil
}

// parseTimestampOrDate parses a point in time that is either given as a Unix
// timestamp in seconds, as an RFC3339 timestamp or as a plain calendar date.
// Dates without a time of day are interpreted as midnight UTC.
func parseTimestampOrDate(value string) (time.Time, error) {
	// A bare number is a Unix timestamp in seconds.
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return time.Unix(seconds, 0).UTC(), nil
	}

	for _, layout := range []string{time.RFC3339, birthdayDateFormat} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("value %q is neither a Unix timestamp "+
		"in seconds, an RFC3339 timestamp nor a %s date", value,
		birthdayDateFormat)
}

// formatBirthday renders a Unix timestamp in seconds as a human readable UTC
// timestamp for logging.
func formatBirthday(birthday uint64) string {
	return time.Unix(int64(birthday), 0).UTC().Format(time.RFC3339)
}

func getNetworkParams(network string) (*chaincfg.Params, error) {
	switch strings.ToLower(network) {
	case "mainnet":
		return &chaincfg.MainNetParams, nil

	case "testnet", "testnet3":
		return &chaincfg.TestNet3Params, nil

	case "regtest":
		return &chaincfg.RegressionNetParams, nil

	case "simnet":
		return &chaincfg.SimNetParams, nil

	default:
		return nil, fmt.Errorf("unknown network: %v", network)
	}
}

func getUnlockerConnection(rpcServer,
	tlsPath string) (lnrpc.WalletUnlockerClient, error) {

	creds, err := credentials.NewClientTLSFromFile(tlsPath, "")
	if err != nil {
		return nil, fmt.Errorf("error loading TLS certificate "+
			"from %s: %v", tlsPath, err)
	}

	// We need to use a custom dialer so we can also connect to unix sockets
	// and not just TCP addresses.
	genericDialer := lncfg.ClientAddressDialer(defaultRPCPort)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithContextDialer(genericDialer),
	}

	conn, err := grpc.NewClient(rpcServer, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to RPC server: %v",
			err)
	}

	return lnrpc.NewWalletUnlockerClient(conn), nil
}
