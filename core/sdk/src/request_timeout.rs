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

use iggy_common::IggyDuration;
use iggy_common::IggyError;
use std::future::Future;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestContext {
    pub timeout: Option<IggyDuration>,
}

tokio::task_local! {
    pub(crate) static REQUEST_CONTEXT: RequestContext;
}

pub(crate) fn get_timeout_override() -> Option<IggyDuration> {
    REQUEST_CONTEXT.try_with(|ctx| ctx.timeout).ok().flatten()
}

pub trait WithTimeout: Future + Sized {
    fn with_timeout(self, timeout: IggyDuration) -> impl Future<Output = Self::Output> + Send
    where
        Self: Send,
    {
        REQUEST_CONTEXT.scope(
            RequestContext {
                timeout: Some(timeout),
            },
            self,
        )
    }
}

impl<F, T> WithTimeout for F where F: Future<Output = Result<T, IggyError>> + Sized {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn task_local_is_none_by_default() {
        assert!(get_timeout_override().is_none());
    }

    #[tokio::test]
    async fn task_local_is_set_inside_scope() {
        let timeout = IggyDuration::from_str("5s").unwrap();
        REQUEST_CONTEXT
            .scope(
                RequestContext {
                    timeout: Some(timeout),
                },
                async {
                    assert_eq!(get_timeout_override(), Some(timeout));
                },
            )
            .await;
    }

    #[tokio::test]
    async fn task_local_is_none_after_scope() {
        let timeout = IggyDuration::from_str("5s").unwrap();
        REQUEST_CONTEXT
            .scope(
                RequestContext {
                    timeout: Some(timeout),
                },
                async {},
            )
            .await;
        assert!(get_timeout_override().is_none());
    }

    #[tokio::test]
    async fn with_timeout_sets_override() {
        let timeout = IggyDuration::from_str("10s").unwrap();
        let result: Result<Option<IggyDuration>, IggyError> = async { Ok(get_timeout_override()) }
            .with_timeout(timeout)
            .await;
        assert_eq!(result.unwrap(), Some(timeout));
    }
}
