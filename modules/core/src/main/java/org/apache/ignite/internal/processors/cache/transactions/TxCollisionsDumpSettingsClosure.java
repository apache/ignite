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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Change tx collisions interval or negative for disabling.
 */
public class TxCollisionsDumpSettingsClosure implements IgniteRunnable {
    /** Serialization ID. */
    private static final long serialVersionUID = 0L;

    /** Auto-inject Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /**
     * Tx key collision dump interval.
     * Check {@link IgniteSystemProperties#IGNITE_DUMP_TX_COLLISIONS_INTERVAL} for additional info.
     */
    private final int interval;

    /** Constructor.
     *
     * @param timeout New interval for key collisions collection.
     */
    TxCollisionsDumpSettingsClosure(int timeout) {
        interval = timeout;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        ignite.context().cache().context().tm().txCollisionsDumpInterval(interval);
    }
}
