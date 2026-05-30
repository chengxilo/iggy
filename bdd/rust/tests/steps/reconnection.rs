/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::sync::Arc;
use std::time::Duration;

use cucumber::{given, then, when};
use iggy::prelude::*;
use iggy_common::Credentials;
use secrecy::SecretString;

use crate::common::reconnect_context::ReconnectContext;
use crate::helpers::chaos;

#[given("I have a running Iggy server whose network may be disrupted")]
async fn given_server_might_disrupt(world: &mut ReconnectContext) {
    world.server_addr =
        std::env::var("IGGY_TCP_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8090".to_string());
    world.chaos_api = std::env::var("IGGY_CHAOS_API_ADDRESS")
        .unwrap_or_else(|_| "http://127.0.0.1:8475".to_string());

    chaos::call_chaos(&world.chaos_api, "resume")
        .await
        .expect("Failed to ensure network is clear");
}

#[given(
    regex = r"^reconnection is (enabled|disabled)(?: with (\d+) retries and (\d+) seconds interval)?$"
)]
async fn set_reconnection(
    world: &mut ReconnectContext,
    state: String,
    retries: String,
    interval_secs: String,
) {
    world.reconnection.enabled = state == "enabled";
    if !world.reconnection.enabled {
        return;
    }
    if !retries.is_empty() {
        world.reconnection.max_retries = Some(retries.parse().expect("invalid retries"));
    }
    if !interval_secs.is_empty() {
        world.reconnection.interval = IggyDuration::from(Duration::from_secs(
            interval_secs.parse().expect("invalid interval"),
        ));
    }
}

#[given(regex = r"^auto-login is (enabled|disabled)$")]
async fn set_auto_login(world: &mut ReconnectContext, state: String) {
    if state == "enabled" {
        world.auto_login = AutoLogin::Enabled(Credentials::UsernamePassword(
            DEFAULT_ROOT_USERNAME.to_string(),
            SecretString::from(DEFAULT_ROOT_PASSWORD.to_string()),
        ));
    } else {
        world.auto_login = AutoLogin::Disabled;
    }
}

#[given("I connect to the server")]
async fn connect_to_server(world: &mut ReconnectContext) {
    let config = TcpClientConfig {
        server_address: world.server_addr.clone(),
        reconnection: world.reconnection.clone(),
        auto_login: world.auto_login.clone(),
        ..TcpClientConfig::default()
    };

    let tcp_client = TcpClient::create(Arc::new(config)).expect("Failed to create TCP client");
    Client::connect(&tcp_client)
        .await
        .expect("Failed to connect");

    let client = IggyClient::create(ClientWrapper::Tcp(tcp_client), None, None);
    world.client = Some(client);
}

#[when("I login as root")]
async fn login_as_root(world: &mut ReconnectContext) {
    let client = world.client.as_ref().expect("Client should exist");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("Failed to login as root");
}

#[when("the network connection is disrupted")]
async fn disrupt(world: &mut ReconnectContext) {
    chaos::call_chaos(&world.chaos_api, "disrupt")
        .await
        .expect("Failed to disrupt network");
}

#[when(regex = r"^the network connection will be restored after (\d+) seconds$")]
async fn will_restore_after(world: &mut ReconnectContext, seconds: u64) {
    let chaos_api = world.chaos_api.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        let _ = chaos::call_chaos(&chaos_api, "resume").await;
    });
}

#[when("the network connection is restored")]
async fn restore(world: &mut ReconnectContext) {
    chaos::call_chaos(&world.chaos_api, "resume")
        .await
        .expect("Failed to restore network");
}

#[when("I manually reconnect to the server")]
async fn manually_reconnect(world: &mut ReconnectContext) {
    let client = world.client.as_ref().expect("Client should exist");
    client
        .connect()
        .await
        .expect("Failed to manually reconnect");
}

#[then(regex = r"^my (?:next )?request should (?:also )?succeed$")]
async fn request_should_succeed(world: &mut ReconnectContext) {
    let client = world.client.as_ref().expect("Client should exist");
    client
        .get_users()
        .await
        .expect("Request should have succeeded");
}

#[when("my request should fail with all attempts failed")]
async fn request_fail_for_all_attempts_fail(world: &mut ReconnectContext) {
    let client = world.client.as_ref().expect("Client should exist");
    let err = client
        .get_users()
        .await
        .expect_err("Expected request to fail");
    assert!(
        matches!(err, IggyError::CannotEstablishConnection),
        "Expected CannotEstablishConnection, got: {err}"
    );
}

#[then(regex = r"^my (?:next )?request should (?:also )?fail with a disconnected error$")]
async fn request_fails_for_disconnected(world: &mut ReconnectContext) {
    let client = world.client.as_ref().expect("Client should exist");
    let err = client
        .get_users()
        .await
        .expect_err("Expected request to fail");
    assert!(
        matches!(err, IggyError::Disconnected),
        "Expected Disconnected, got: {err}"
    );
}

#[then(regex = r"^my (?:next )?request should (?:also )?fail with an authentication error$")]
async fn request_fails_for_unauthenticated(world: &mut ReconnectContext) {
    let client = world.client.as_ref().expect("Client should exist");
    let err = client
        .get_users()
        .await
        .expect_err("Expected request to fail");
    assert!(
        matches!(err, IggyError::Unauthenticated),
        "Expected Unauthenticated, got: {err}"
    );
}
