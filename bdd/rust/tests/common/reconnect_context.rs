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

use cucumber::World;
use iggy::prelude::{AutoLogin, IggyClient, TcpClientReconnectionConfig};

#[derive(Debug, World)]
pub struct ReconnectContext {
    pub server_addr: String,
    pub chaos_api: String,
    pub client: Option<IggyClient>,
    pub reconnection: TcpClientReconnectionConfig,
    pub auto_login: AutoLogin,
}

impl Default for ReconnectContext {
    fn default() -> Self {
        Self {
            server_addr: String::new(),
            chaos_api: String::new(),
            client: None,
            reconnection: TcpClientReconnectionConfig::default(),
            auto_login: AutoLogin::Disabled,
        }
    }
}
