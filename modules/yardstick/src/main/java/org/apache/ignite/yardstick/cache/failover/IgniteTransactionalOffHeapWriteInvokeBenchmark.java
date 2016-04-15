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
 * Transactional write invoke failover benchmark.
 * <p>
 * Each client generates a random integer K in a limited range and creates keys in the form 'key-' + K + 'master',
 * 'key-' + K + '-1', 'key-' + K + '-2', ... Then client starts a pessimistic repeatable read transaction
 * and randomly chooses between read and write scenarios:
 * <ul>
 * <li>Reads value associated with the master key and child keys. Values must be equal.</li>
 * <li>Reads value associated with the master key, increments it by 1 and puts the value, then invokes increment
 * closure on child keys. No validation is performed.</li>
 * </ul>
 */
public class IgniteTransactionalOffHeapWriteInvokeBenchmark extends IgniteTransactionalWriteInvokeBenchmark {
    /** {@inheritDoc} */
    @Override protected String cacheName() {
        return "tx-offheap-write-invoke";
    }
}
