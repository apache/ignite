/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

/**
 * Retry policy that returns true for all read-only operations that do not modify data.
 */
public class ClientRetryReadPolicy implements ClientRetryPolicy {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public boolean shouldRetry(ClientRetryPolicyContext context) {
        switch (context.operation()) {
            case CACHE_GET_NAMES:
            case CACHE_GET:
            case CACHE_CONTAINS_KEY:
            case CACHE_CONTAINS_KEYS:
            case CACHE_GET_CONFIGURATION:
            case CACHE_GET_SIZE:
            case CACHE_GET_ALL:
            case QUERY_SCAN:
            case QUERY_CONTINUOUS:
            case CLUSTER_GET_STATE:
            case CLUSTER_GET_WAL_STATE:
            case CLUSTER_GROUP_GET_NODES:
            case SERVICE_GET_DESCRIPTORS:
            case SERVICE_GET_DESCRIPTOR:
                return true;

            default:
                return false;
        }
    }
}
