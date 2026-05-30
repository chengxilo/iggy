# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@requires-network-chaos
Feature: TCP Client Reconnection and Auto-Login Behavior
  As a developer using Apache Iggy
  I want the client to handle connection losses and auto-login correctly
  So that reconnection-enabled clients recover and reconnection-disabled clients fail fast

  The connection disruption is performed by an iptables sidecar that shares
  the Iggy server's network namespace. All SDKs control it via its HTTP
  API, making the tests language-agnostic and covering every transport protocol.

  Scenario: Client with reconnection and auto-login keep working through disruptions
    Given I have a running Iggy server whose network may be disrupted
    And reconnection is enabled
    And auto-login is enabled
    And I connect to the server
    Then my request should succeed
    When the network connection is disrupted
    And the network connection will be restored after 10 seconds
    Then my request should succeed
    Then my next request should also succeed


  Scenario: Client with reconnection, limited retries but no auto-login
    Given I have a running Iggy server whose network may be disrupted
    And reconnection is enabled with 3 retries and 2 seconds interval
    And auto-login is disabled
    And I connect to the server
    When I login as root
    When the network connection is disrupted
    And my request should fail with all attempts failed
    And the network connection is restored
    Then my next request should fail with an authentication error
    When I login as root
    Then my request should succeed

  Scenario: Client with reconnection, limited retries and auto-login
    Given I have a running Iggy server whose network may be disrupted
    And reconnection is enabled with 3 retries and 2 seconds interval
    And auto-login is enabled
    And I connect to the server
    When the network connection is disrupted
    Then my request should fail with all attempts failed
    When the network connection is restored
    Then my request should succeed

  Scenario: Client with reconnection disabled and auto-login enabled
    Given I have a running Iggy server whose network may be disrupted
    And reconnection is disabled
    And auto-login is enabled
    And I connect to the server
    When the network connection is disrupted
    Then my request should fail with a disconnected error
    And the network connection is restored
    Then my next request should also fail with a disconnected error
    When I manually reconnect to the server
    Then my request should succeed

  Scenario: Brief outage within retry budget without auto-login
    Given I have a running Iggy server whose network may be disrupted
    And reconnection is enabled with 5 retries and 2 seconds interval
    And auto-login is disabled
    And I connect to the server
    When I login as root
    Then my request should succeed
    When the network connection is disrupted
    And the network connection will be restored after 3 seconds
    Then my request should fail with an authentication error
    When I login as root
    Then my request should succeed

  Scenario: Client survives multiple consecutive disruptions with both reconnection and auto-login enabled
    Given I have a running Iggy server whose network may be disrupted
    And reconnection is enabled
    And auto-login is enabled
    And I connect to the server
    Then my request should succeed
    When the network connection is disrupted
    And the network connection will be restored after 5 seconds
    Then my request should succeed
    When the network connection is disrupted
    And the network connection will be restored after 5 seconds
    Then my request should succeed

  Scenario: Custom retry interval prevents retry exhaustion during short outage
    Given I have a running Iggy server whose network may be disrupted
    And reconnection is enabled with 2 retries and 5 seconds interval
    And auto-login is enabled
    And I connect to the server
    Then my request should succeed
    When the network connection is disrupted
    And the network connection will be restored after 3 seconds
    Then my request should succeed
