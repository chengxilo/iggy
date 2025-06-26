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

package iggycli

import (
	"context"
	"fmt"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/tcp"
)

type IggyClientBuilder struct {
	client Client
}

// NewIggyClientBuilder create a new IggyClientBuilder
// This is not enough to build the IggyClient instance. You need to provide the client configuration or the client
// implementation for the specific transport.
func NewIggyClientBuilder() *IggyClientBuilder {
	return &IggyClientBuilder{}
}

// WithClient apply the provided client implementation for the specific transport. Setting client clears the client config.
func (b *IggyClientBuilder) WithClient(client Client) *IggyClientBuilder {
	b.client = client
	return b
}

// WithTcp provides fluent API for the TCP client configuration.
// It returns the TcpClientBuilder instance, which allows to configure the TCP client with custom settings or using defaults.
func (b *IggyClientBuilder) WithTcp() *TcpClientBuilder {
	return NewTcpClientBuilder()
}

// Build the IggyClient instance.
// This method returns ierror.InvalidConfiguration if the client is not provided.
// If the client is provided, it creates the IggyClient instance with the provided configuration.
// To provide the client configuration, use the WithTcp, WithQuic, WIthHttp methods (WithQuic and WithHttp are not implemented yet).
func (b *IggyClientBuilder) Build() (*IggyClient, error) {
	if b.client == nil {
		return nil, fmt.Errorf("iggycli is not provided: %w", ierror.InvalidConfiguration)
	}
	return CreateIggyClient(b.client), nil
}

type TcpClientBuilder struct {
	config        tcp.ClientConfig
	parentBuilder IggyClientBuilder
}

// NewTcpClientBuilder create a new TcpClientBuilder
func NewTcpClientBuilder() *TcpClientBuilder {
	return &TcpClientBuilder{}
}

// WithServerAddress Sets the server address for the TCP client.
func (b *TcpClientBuilder) WithServerAddress(address string) *TcpClientBuilder {
	b.config.ServerAddress = address
	return b
}

// WithContext sets context
func (b *TcpClientBuilder) WithContext(ctx context.Context) *TcpClientBuilder {
	b.config.Ctx = ctx
	return b
}

// Build builds the parent IggyClient with TCP configuration
func (b *TcpClientBuilder) Build() (*IggyClient, error) {
	b.config = tcp.GetDefaultTcpClientConfig()
	cli, err := tcp.NewIggyTcpClient(b.config)
	if err != nil {
		return nil, err
	}
	return b.parentBuilder.WithClient(cli).Build()
}
