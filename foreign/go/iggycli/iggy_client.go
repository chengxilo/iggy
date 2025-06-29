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
	"fmt"

	. "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/tcp"
)

type IggyClient struct {
	client Client
}

type Options struct {
	protocol   Protocol
	client     Client
	tcpOptions []tcp.Option
}

func GetDefaultOptions() Options {
	return Options{
		protocol:   Tcp,
		client:     nil,
		tcpOptions: nil,
	}
}

type Option func(*Options)

// WithTcp sets the client protocol to TCP and applies custom TCP options.
// If no options are provided, the default TCP client configuration will be used.
func WithTcp(tcpOpts ...tcp.Option) Option {
	return func(opts *Options) {
		opts.protocol = Tcp
		opts.tcpOptions = tcpOpts
	}
}

// NewIggyClient create the IggyClient instance.
// If no Option is provided, NewIggyClient will create a default TCP client.
func NewIggyClient(options ...Option) (*IggyClient, error) {
	opts := GetDefaultOptions()

	for _, opt := range options {
		opt(&opts)
	}

	var err error
	var cli Client
	switch opts.protocol {
	case Tcp:
		cli, err = tcp.NewIggyTcpClient(opts.tcpOptions...)
	default:
		return nil, fmt.Errorf("unknown protocol type: %v", opts.protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create an iggy client: %w", err)
	}

	return &IggyClient{
		client: cli,
	}, nil
}

func (c IggyClient) GetStream(request GetStreamRequest) (*StreamResponse, error) {
	return c.client.GetStream(request)
}

func (c IggyClient) GetStreams() ([]StreamResponse, error) {
	return c.client.GetStreams()
}

func (c IggyClient) CreateStream(request CreateStreamRequest) error {
	return c.client.CreateStream(request)
}

func (c IggyClient) UpdateStream(request UpdateStreamRequest) error {
	return c.client.UpdateStream(request)
}

func (c IggyClient) DeleteStream(streamId Identifier) error {
	return c.client.DeleteStream(streamId)
}

func (c IggyClient) GetTopic(streamId, topicId Identifier) (*TopicDetails, error) {
	return c.client.GetTopic(streamId, topicId)
}

func (c IggyClient) GetTopics(streamId Identifier) ([]Topic, error) {
	return c.client.GetTopics(streamId)
}

func (c IggyClient) CreateTopic(request CreateTopicRequest) error {
	return c.client.CreateTopic(request)
}

func (c IggyClient) UpdateTopic(request UpdateTopicRequest) error {
	return c.client.UpdateTopic(request)
}

func (c IggyClient) DeleteTopic(streamId, topicId Identifier) error {
	return c.client.DeleteTopic(streamId, topicId)
}

func (c IggyClient) SendMessages(request SendMessagesRequest) error {
	if len(request.Messages) == 0 {
		return ierror.CustomError("messages_count_should_be_greater_than_zero")
	}
	return c.client.SendMessages(request)
}

func (c IggyClient) PollMessages(request PollMessageRequest) (*PollMessageResponse, error) {
	return c.client.PollMessages(request)
}

func (c IggyClient) StoreConsumerOffset(request StoreConsumerOffsetRequest) error {
	return c.client.StoreConsumerOffset(request)
}

func (c IggyClient) GetConsumerOffset(request GetConsumerOffsetRequest) (*ConsumerOffsetInfo, error) {
	return c.client.GetConsumerOffset(request)
}

func (c IggyClient) GetConsumerGroup(streamId, topicId, groupId Identifier) (*ConsumerGroupResponse, error) {
	return c.client.GetConsumerGroup(streamId, topicId, groupId)
}

func (c IggyClient) GetConsumerGroups(streamId Identifier, topicId Identifier) ([]ConsumerGroupResponse, error) {
	return c.client.GetConsumerGroups(streamId, topicId)
}

func (c IggyClient) CreateConsumerGroup(request CreateConsumerGroupRequest) error {
	return c.client.CreateConsumerGroup(request)
}

func (c IggyClient) DeleteConsumerGroup(request DeleteConsumerGroupRequest) error {
	return c.client.DeleteConsumerGroup(request)
}

func (c IggyClient) JoinConsumerGroup(request JoinConsumerGroupRequest) error {
	return c.client.JoinConsumerGroup(request)
}

func (c IggyClient) LeaveConsumerGroup(request LeaveConsumerGroupRequest) error {
	return c.client.LeaveConsumerGroup(request)
}

func (c IggyClient) CreatePartitions(request CreatePartitionsRequest) error {
	return c.client.CreatePartitions(request)
}

func (c IggyClient) DeletePartitions(request DeletePartitionsRequest) error {
	return c.client.DeletePartitions(request)
}

func (c IggyClient) GetUser(identifier Identifier) (*UserInfoDetails, error) {
	return c.client.GetUser(identifier)
}

func (c IggyClient) GetUsers() ([]*UserInfo, error) {
	return c.client.GetUsers()
}

func (c IggyClient) CreateUser(request CreateUserRequest) error {
	return c.client.CreateUser(request)
}

func (c IggyClient) UpdateUser(request UpdateUserRequest) error {
	return c.client.UpdateUser(request)
}

func (c IggyClient) UpdatePermissions(request UpdatePermissionsRequest) error {
	return c.client.UpdatePermissions(request)
}

func (c IggyClient) ChangePassword(request ChangePasswordRequest) error {
	return c.client.ChangePassword(request)
}

func (c IggyClient) DeleteUser(identifier Identifier) error {
	return c.client.DeleteUser(identifier)
}

func (c IggyClient) CreatePersonalAccessToken(request CreatePersonalAccessTokenRequest) (*AccessToken, error) {
	return c.client.CreatePersonalAccessToken(request)
}

func (c IggyClient) DeletePersonalAccessToken(request DeletePersonalAccessTokenRequest) error {
	return c.client.DeletePersonalAccessToken(request)
}

func (c IggyClient) GetPersonalAccessTokens() ([]PersonalAccessTokenInfo, error) {
	return c.client.GetPersonalAccessTokens()
}

func (c IggyClient) LoginUser(request LoginUserRequest) (*LoginUserResponse, error) {
	return c.client.LoginUser(request)
}

func (c IggyClient) LoginWithPersonalAccessToken(request LoginWithPersonalAccessTokenRequest) (*LoginUserResponse, error) {
	return c.client.LoginWithPersonalAccessToken(request)
}

func (c IggyClient) LogoutUser() error {
	return c.client.LogoutUser()
}

func (c IggyClient) GetStats() (*Stats, error) {
	return c.client.GetStats()
}

func (c IggyClient) Ping() error {
	return c.client.Ping()
}

func (c IggyClient) GetClients() ([]ClientResponse, error) {
	return c.client.GetClients()
}

func (c IggyClient) GetClient(clientId int) (*ClientResponse, error) {
	return c.client.GetClient(clientId)
}
