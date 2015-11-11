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

package org.apache.ignite.yardstick.cache.failover;

/**
 * Invoke retry failover benchmark.
 * <p>
 * Each client maintains a local map that it updates together with cache.
 * Client invokes an increment closure for all generated keys and atomically increments value for corresponding
 * keys in the local map. To validate cache contents, all writes from the client are stopped, values in
 * the local map are compared to the values in the cache.
 */
public class IgniteTransactionalOffHeapInvokeRetryBenchmark extends IgniteTransactionalInvokeRetryBenchmark {
    /** {@inheritDoc} */
    @Override protected String cacheName() {
        return "tx-offheap-invoke-retry";
    }
}
