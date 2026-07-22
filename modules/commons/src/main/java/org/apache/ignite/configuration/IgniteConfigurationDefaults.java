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

package org.apache.ignite.configuration;

/** */
public interface IgniteConfigurationDefaults {
    /** Default rebalance batch size in bytes (value is {@code 512Kb}). */
    public static final int DFLT_REBALANCE_BATCH_SIZE = 512 * 1024; // 512K

    /** Default rebalance batches prefetch count (value is {@code 3}). */
    public static final long DFLT_REBALANCE_BATCHES_PREFETCH_COUNT = 3;

    /** Time to wait between rebalance messages in milliseconds to avoid overloading CPU (value is {@code 0}). */
    public static final long DFLT_REBALANCE_THROTTLE = 0;

    /** Default rebalance message timeout in milliseconds (value is {@code 10000}). */
    public static final long DFLT_REBALANCE_TIMEOUT = 10000;
}
