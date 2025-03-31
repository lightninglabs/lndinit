package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/signal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	connectionRetryInterval = time.Millisecond * 250
)

type waitReadyCommand struct {
	RPCServer string        `long:"rpcserver" description:"The host:port of lnd's RPC listener"`
	Timeout   time.Duration `long:"timeout" description:"The maximum time we'll wait for lnd to become ready; 0 means wait forever"`
}

func newWaitReadyCommand() *waitReadyCommand {
	return &waitReadyCommand{
		RPCServer: defaultRPCServer,
	}
}

func (x *waitReadyCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"wait-ready",
		"Wait for lnd to be fully ready",
		"Wait for lnd to be fully started, unlocked and ready to "+
			"serve RPC requests; will wait and block forever "+
			"until either lnd reports its status as ready or the "+
			"given timeout is reached; the RPC connection to lnd "+
			"is re-tried indefinitely and errors are ignored (or "+
			"logged in verbose mode) until success or timeout; "+
			"requires lnd v0.13.0-beta or later to work",
		x,
	)
	return err
}

func (x *waitReadyCommand) Execute(_ []string) error {
	// Since this will potentially run forever, make sure we catch any
	// interrupt signals.
	shutdown, err := signal.Intercept()
	if err != nil {
		return fmt.Errorf("error intercepting signals: %v", err)
	}

	started := time.Now()
	timeout := time.Duration(math.MaxInt64)
	if x.Timeout > 0 {
		timeout = x.Timeout
		logger.Infof("Will time out in %v (%s)", timeout, started.Add(timeout))
	}

	return waitUntilStatus(
		x.RPCServer, lnrpc.WalletState_SERVER_ACTIVE, timeout,
		shutdown.ShutdownChannel(),
	)
}

func waitUntilStatus(rpcServer string, desiredState lnrpc.WalletState,
	timeout time.Duration, shutdown <-chan struct{}) error {

	logger.Infof("Waiting for lnd to become ready (want state %v)", desiredState)

	connectionRetryTicker := time.NewTicker(connectionRetryInterval)
	timeoutChan := time.After(timeout)

connectionLoop:
	for {
		logger.Infof("Attempting to connect to RPC server %s", rpcServer)
		conn, err := getStatusConnection(rpcServer)
		if err != nil {
			logger.Errorf("Connection to lnd not successful: %v", err)

			select {
			case <-connectionRetryTicker.C:
			case <-timeoutChan:
				return fmt.Errorf("timeout reached")
			case <-shutdown:
				return nil
			}

			continue
		}

		logger.Info("Attempting to subscribe to the wallet state")
		statusStream, err := conn.SubscribeState(
			context.Background(), &lnrpc.SubscribeStateRequest{},
		)
		if err != nil {
			logger.Errorf("Status subscription for lnd not successful: %v",
				err)

			select {
			case <-connectionRetryTicker.C:
			case <-timeoutChan:
				return fmt.Errorf("timeout reached")
			case <-shutdown:
				return nil
			}

			continue
		}

		for {
			// Have we reached the global timeout yet?
			select {
			case <-timeoutChan:
				return fmt.Errorf("timeout reached")
			case <-shutdown:
				return nil
			default:
			}

			msg, err := statusStream.Recv()
			if err != nil {
				logger.Errorf("Error receiving status update: %v", err)

				select {
				case <-connectionRetryTicker.C:
				case <-timeoutChan:
					return fmt.Errorf("timeout reached")
				case <-shutdown:
					return nil
				}

				// Something went wrong, perhaps lnd shut down
				// before becoming active. Let's retry the whole
				// connection again.
				continue connectionLoop
			}

			logger.Infof("Received update from lnd, wallet status is now: "+
				"%v", msg.State)

			// We've arrived at the final state!
			if msg.State == desiredState {
				return nil
			}

			// If we're waiting for a state that is at the very
			// beginning of the list (e.g. NON_EXISTENT) then we
			// need to return an error if a state further down the
			// list is returned, as that would mean we skipped over
			// it. The only exception is the WAITING_TO_START since
			// that is actually the largest number a state can have.
			if msg.State != lnrpc.WalletState_WAITING_TO_START &&
				msg.State > desiredState {

				return fmt.Errorf("received state %v which "+
					"is greater than %v", msg.State,
					desiredState)
			}

			// Let's wait for another status message to arrive.
		}
	}
}

func getStatusConnection(rpcServer string) (lnrpc.StateClient, error) {
	// Don't bother with checking the cert, we're not sending any macaroons
	// to the server anyway.
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})

	// We need to use a custom dialer so we can also connect to unix sockets
	// and not just TCP addresses.
	genericDialer := lncfg.ClientAddressDialer(defaultRPCPort)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithContextDialer(genericDialer),
	}

	conn, err := grpc.Dial(rpcServer, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to RPC server: %v",
			err)
	}

	return lnrpc.NewStateClient(conn), nil
}
