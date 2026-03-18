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

package tcp

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
)

// newTestClient creates an IggyTcpClient backed by an in-memory net.Pipe connection.
// Returns the client and the server-side end of the pipe; caller must close the server conn.
func newTestClient(t *testing.T) (*IggyTcpClient, net.Conn) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	c := &IggyTcpClient{
		conn:  clientConn,
		state: iggcon.StateConnected,
	}
	t.Cleanup(func() {
		err := clientConn.Close()
		if err != nil {
			t.Errorf("error closing client connection: %v", err)
		}
	})
	t.Cleanup(func() {
		err := serverConn.Close()
		if err != nil {
			t.Errorf("error closing server connection: %v", err)
		}
	})
	return c, serverConn
}

func TestSendAndFetchResponse_NilContext(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.sendAndFetchResponse(nil, []byte{}, command.Code(0))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "nil context") {
		t.Errorf("got %q, want it to contain %q", err.Error(), "nil context")
	}
}

func TestSendAndFetchResponse_ContextErrors(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	expiredCtx, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expiredCancel()

	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{
			name:    "canceled",
			ctx:     canceledCtx,
			wantErr: context.Canceled,
		},
		{
			name:    "expired",
			ctx:     expiredCtx,
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newTestClient(t)

			// server does not respond, but it doesn't matter.
			// ctx.Err() should fire before any I/O is attempted.
			_, err := c.sendAndFetchResponse(tt.ctx, []byte{}, command.Code(0))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestSendAndFetchResponse_DeadlineTimeout(t *testing.T) {
	c, _ := newTestClient(t)

	// server intentionally does not read or write, causing the client to block
	// until the context deadline fires and SetDeadline triggers a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := c.sendAndFetchResponse(ctx, []byte{}, command.Code(0))
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	var netErr net.Error
	if !errors.As(err, &netErr) {
		t.Fatalf("expected a net.Error, got %T: %v", err, err)
	}
	if !netErr.Timeout() {
		t.Errorf("expected timeout error, got %v", err)
	}
}
