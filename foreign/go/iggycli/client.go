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
	"time"

	. "github.com/apache/iggy/foreign/go/contracts"
)

type Client interface {
	GetStream(streamId Identifier) (*StreamDetails, error)
	GetStreams() ([]Stream, error)

	// CreateStream create a new stream.
	// Authentication is required, and the permission to manage the streams.
	CreateStream(name string, streamId *uint32) (*StreamDetails, error)

	// UpdateStream update a stream by unique ID or name.
	// Authentication is required, and the permission to manage the streams.
	UpdateStream(streamId Identifier, name string) error
	DeleteStream(id Identifier) error

	GetTopic(streamId, topicId Identifier) (*TopicDetails, error)
	GetTopics(streamId Identifier) ([]Topic, error)
	CreateTopic(
		streamId Identifier,
		name string,
		partitionsCount int,
		compressionAlgorithm uint8,
		messageExpiry time.Duration,
		maxTopicSize uint64,
		replicationFactor *uint8,
		topicId *int,
	) (*TopicDetails, error)
	// UpdateTopic update a topic by unique ID or name.
	// Authentication is required, and the permission to manage the topics.
	UpdateTopic(
		streamId Identifier,
		topicId Identifier,
		name string,
		compressionAlgorithm uint8,
		messageExpiry time.Duration,
		maxTopicSize uint64,
		replicationFactor *uint8,
	) error
	DeleteTopic(streamId, topicId Identifier) error
	// SendMessages sends messages using specified partitioning strategy to the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to send the messages.
	SendMessages(
		streamId Identifier,
		topicId Identifier,
		partitioning Partitioning,
		messages []IggyMessage,
	) error
	// PollMessages poll given amount of messages using the specified consumer and strategy from the specified stream and topic by unique IDs or names.
	// Authentication is required, and the permission to poll the messages.
	PollMessages(
		streamId Identifier,
		topicId Identifier,
		consumer Consumer,
		strategy PollingStrategy,
		count uint32,
		autoCommit bool,
		partitionId *uint32,
	) (*PolledMessage, error)

	StoreConsumerOffset(request StoreConsumerOffsetRequest) error
	GetConsumerOffset(request GetConsumerOffsetRequest) (*ConsumerOffsetInfo, error)
	GetConsumerGroups(streamId Identifier, topicId Identifier) ([]ConsumerGroup, error)
	GetConsumerGroup(streamId, topicId, groupId Identifier) (*ConsumerGroupDetails, error)
	CreateConsumerGroup(request CreateConsumerGroupRequest) error
	DeleteConsumerGroup(request DeleteConsumerGroupRequest) error
	JoinConsumerGroup(request JoinConsumerGroupRequest) error
	LeaveConsumerGroup(request LeaveConsumerGroupRequest) error

	CreatePartitions(request CreatePartitionsRequest) error
	DeletePartitions(request DeletePartitionsRequest) error

	GetUser(identifier Identifier) (*UserInfoDetails, error)
	GetUsers() ([]*UserInfo, error)
	CreateUser(request CreateUserRequest) error
	UpdateUser(request UpdateUserRequest) error
	UpdatePermissions(request UpdatePermissionsRequest) error
	ChangePassword(request ChangePasswordRequest) error
	DeleteUser(identifier Identifier) error

	CreatePersonalAccessToken(request CreatePersonalAccessTokenRequest) (*AccessToken, error)
	DeletePersonalAccessToken(request DeletePersonalAccessTokenRequest) error
	GetPersonalAccessTokens() ([]PersonalAccessTokenInfo, error)
	LoginWithPersonalAccessToken(request LoginWithPersonalAccessTokenRequest) (*LoginUserResponse, error)

	LoginUser(request LoginUserRequest) (*LoginUserResponse, error)
	LogoutUser() error

	GetStats() (*Stats, error)
	Ping() error

	GetClients() ([]ClientResponse, error)
	GetClient(clientId int) (*ClientResponse, error)
}
