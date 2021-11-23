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
 * Retry policy that returns true for all idempotent operations.
 * <p>
 * Idempotent operations can be performed multiple times without changing the result beyond the initial application.
 * For example, cache.put is idempotent, we can put the same value multiple times and the result will be the same. It is safe to retry
 * an idempotent operation. On the other hand, an SQL query that transfers funds from one account into another is not idempotent. It is
 * not safe to retry, because previous query could have completed, but the connection failed during the response.
 */
public class ClientRetryIdempotentPolicy implements ClientRetryPolicy {
    /** {@inheritDoc} */
    @Override public boolean shouldRetry(
            IgniteClient client,
            ClientOperationType operationType,
            int iteration,
            ClientConnectionException exception) {
        // TODO: Full list of idempotent operations.
        return operationType == ClientOperationType.CACHE_GET || operationType == ClientOperationType.CACHE_PUT;
    }
}
