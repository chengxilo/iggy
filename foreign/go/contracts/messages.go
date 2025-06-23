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

package iggcon

import "errors"

const (
	MaxPayloadSize     = 10 * 1000 * 1000
	MaxUserHeadersSize = 100 * 1000
)

type FetchMessagesRequest struct {
	StreamId        Identifier      `json:"streamId"`
	TopicId         Identifier      `json:"topicId"`
	Consumer        Consumer        `json:"consumer"`
	PartitionId     int             `json:"partitionId"`
	PollingStrategy PollingStrategy `json:"pollingStrategy"`
	Count           int             `json:"count"`
	AutoCommit      bool            `json:"autoCommit"`
}

type FetchMessagesResponse struct {
	PartitionId   uint32
	CurrentOffset uint64
	MessageCount  uint32
	Messages      []IggyMessage
}

type SendMessagesRequest struct {
	StreamId     Identifier    `json:"streamId"`
	TopicId      Identifier    `json:"topicId"`
	Partitioning Partitioning  `json:"partitioning"`
	Messages     []IggyMessage `json:"messages"`
}

type ReceivedMessage struct {
	Message       IggyMessage
	CurrentOffset uint64
	PartitionId   uint32
}

type IggyMessage struct {
	Header      MessageHeader
	Payload     []byte
	UserHeaders []byte
}

type IggyMessageOpt func(message *IggyMessage)

var ErrInvalidMessagePayloadLength = errors.New("invalid message payload length")
var ErrTooBigUserMessagePayload = errors.New("too big message payload")
var ErrTooBigUserHeaders = errors.New("too big headers payload")

// NewIggyMessage Creates a new message with customizable parameters.
func NewIggyMessage(payload []byte, opts ...IggyMessageOpt) (IggyMessage, error) {
	if len(payload) == 0 {
		return IggyMessage{}, ErrInvalidMessagePayloadLength
	}

	if len(payload) > MaxPayloadSize {
		return IggyMessage{}, ErrTooBigUserMessagePayload
	}

	header := NewMessageHeader([16]byte{}, uint32(len(payload)), 0)
	message := IggyMessage{
		Header:      header,
		Payload:     payload,
		UserHeaders: make([]byte, 0),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&message)
		}
	}
	userHeaderLength := len(message.UserHeaders)
	if userHeaderLength > MaxUserHeadersSize {
		return IggyMessage{}, ErrTooBigUserHeaders
	}
	message.Header.UserHeaderLength = uint32(userHeaderLength)
	return message, nil
}

func WithID(id [16]byte) IggyMessageOpt {
	return func(m *IggyMessage) {
		m.Header.Id = id
	}
}

func WithUserHeaders(userHeaders map[HeaderKey]HeaderValue) IggyMessageOpt {
	return func(m *IggyMessage) {
		userHeaderBytes := GetHeadersBytes(userHeaders)
		m.UserHeaders = userHeaderBytes
	}
}
