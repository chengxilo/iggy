// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tests

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/cucumber/godog"
)

type reconnectCtxKey struct{}

type reconnectCtx struct {
	serverAddr string
	chaosAPI   string
	client     iggcon.Client
	tcpOpts    []tcp.Option
}

func getReconnectCtx(ctx context.Context) *reconnectCtx {
	return ctx.Value(reconnectCtxKey{}).(*reconnectCtx)
}

func callChaosAPI(baseURL, action string) error {
	resp, err := http.Post(baseURL+"/"+action, "", nil)
	if err != nil {
		return fmt.Errorf("chaos %s: %w", action, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("chaos %s returned %d", action, resp.StatusCode)
	}
	return nil
}

type reconnectSteps struct{}

func (s reconnectSteps) givenServerMightDisrupt(ctx context.Context) error {
	c := getReconnectCtx(ctx)

	c.serverAddr = os.Getenv("IGGY_TCP_ADDRESS")
	if c.serverAddr == "" {
		c.serverAddr = "127.0.0.1:8090"
	}
	c.chaosAPI = os.Getenv("IGGY_CHAOS_API_ADDRESS")
	if c.chaosAPI == "" {
		c.chaosAPI = "http://127.0.0.1:8475"
	}

	if err := callChaosAPI(c.chaosAPI, "resume"); err != nil {
		return fmt.Errorf("ensure network clear: %w", err)
	}
	return nil
}

func (s reconnectSteps) setReconnection(ctx context.Context, state string, retries string, interval string) error {
	c := getReconnectCtx(ctx)

	enable := state == "enabled"
	var opts []tcp.ReconnectionOption

	// TODO: currently godog doesn't support optional int/uint parameters, change to proper optional parameters once godog supports it
	if retries != "" {
		maxRetries, _ := strconv.ParseUint(retries, 10, 32)
		opts = append(opts, tcp.WithMaxRetries(uint32(maxRetries)))
	}
	if interval != "" {
		secs, _ := strconv.Atoi(interval)
		opts = append(opts, tcp.WithRetryInterval(time.Duration(secs)*time.Second))
	}

	if !enable && len(opts) != 0 {
		return fmt.Errorf("reconnection disabled but other options are provided, this test case is invalid")
	}

	c.tcpOpts = append(c.tcpOpts, tcp.WithReconnection(enable, opts...))
	return nil
}

func (s reconnectSteps) setAutoLogin(ctx context.Context, state string) error {
	c := getReconnectCtx(ctx)
	if state == "enabled" {
		c.tcpOpts = append(c.tcpOpts, tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials("iggy", "iggy")))
	}
	return nil
}

func (s reconnectSteps) connectToServer(ctx context.Context) error {
	c := getReconnectCtx(ctx)
	opts := []tcp.Option{tcp.WithServerAddress(c.serverAddr)}
	opts = append(opts, c.tcpOpts...)
	cli, err := client.NewIggyClient(client.WithTcp(opts...))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	if err = cli.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	c.client = cli
	return nil
}

func (s reconnectSteps) manuallyReconnect(ctx context.Context) error {
	c := getReconnectCtx(ctx)
	if err := c.client.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	return nil
}

func (s reconnectSteps) loginAsRoot(ctx context.Context) error {
	c := getReconnectCtx(ctx)
	if _, err := c.client.LoginUser(ctx, "iggy", "iggy"); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	return nil
}

func (s reconnectSteps) disrupt(ctx context.Context) error {
	return callChaosAPI(getReconnectCtx(ctx).chaosAPI, "disrupt")
}

func (s reconnectSteps) requestShouldFailForAllAttemptsFail(ctx context.Context) error {
	c := getReconnectCtx(ctx)
	if _, err := c.client.GetUsers(ctx); err == nil || !strings.Contains(err.Error(), "All attempts fail") {
		return fmt.Errorf("expected 'All attempts fail' error, got: %v", err)
	}
	return nil
}

func (s reconnectSteps) requestShouldSucceed(ctx context.Context) error {
	c := getReconnectCtx(ctx)
	if _, err := c.client.GetUsers(ctx); err != nil {
		return fmt.Errorf("expected nil, got: %w", err)
	}
	return nil
}

func (s reconnectSteps) willRestoreAfter(ctx context.Context, seconds int) error {
	c := getReconnectCtx(ctx)
	go func() {
		time.Sleep(time.Duration(seconds) * time.Second)
		_ = callChaosAPI(c.chaosAPI, "resume")
	}()
	return nil
}

func (s reconnectSteps) restore(ctx context.Context) error {
	return callChaosAPI(getReconnectCtx(ctx).chaosAPI, "resume")
}

func (s reconnectSteps) requestFailsForDisconnected(ctx context.Context) error {
	c := getReconnectCtx(ctx)
	_, err := c.client.GetUsers(ctx)
	if !errors.Is(err, ierror.ErrDisconnected) {
		return fmt.Errorf("expected disconnected error, got: %v", err)
	}
	return nil
}

func (s reconnectSteps) requestFailsForUnauthenticated(ctx context.Context) error {
	c := getReconnectCtx(ctx)
	_, err := c.client.GetUsers(ctx)
	if !errors.Is(err, ierror.ErrUnauthenticated) {
		return fmt.Errorf("expected unauthenticated error, got: %v", err)
	}
	return nil
}

func initReconnectionScenario(sc *godog.ScenarioContext) {
	sc.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		return context.WithValue(context.Background(), reconnectCtxKey{}, &reconnectCtx{}), nil
	})

	s := reconnectSteps{}
	sc.Step(`^I have a running Iggy server whose network may be disrupted$`, s.givenServerMightDisrupt)
	sc.Step(`^I manually reconnect to the server$`, s.manuallyReconnect)
	sc.Step(`^reconnection is (enabled|disabled)(?: with (\d+) retries and (\d+) seconds interval)?$`, s.setReconnection)
	sc.Step(`^auto-login is (enabled|disabled)$`, s.setAutoLogin)
	sc.Step(`^I connect to the server$`, s.connectToServer)
	sc.Step(`^I login as root$`, s.loginAsRoot)
	sc.Step(`^the network connection is disrupted$`, s.disrupt)
	sc.Step(`^the network connection will be restored after (\d+) seconds$`, s.willRestoreAfter)
	sc.Step(`^the network connection is restored$`, s.restore)
	sc.Step(`^my (?:next )?request should (?:also )?succeed$`, s.requestShouldSucceed)
	sc.Step(`^my request should fail with all attempts failed$`, s.requestShouldFailForAllAttemptsFail)
	sc.Step(`^my (?:next )?request should (?:also )?fail with a disconnected error$`, s.requestFailsForDisconnected)
	sc.Step(`^my (?:next )?request should (?:also )?fail with an authentication error$`, s.requestFailsForUnauthenticated)

	sc.After(func(ctx context.Context, sc *godog.Scenario, scErr error) (context.Context, error) {
		c := getReconnectCtx(ctx)
		if c.chaosAPI != "" {
			_ = callChaosAPI(c.chaosAPI, "resume")
		}
		if c.client != nil {
			_ = c.client.Close()
		}
		return ctx, scErr
	})
}
