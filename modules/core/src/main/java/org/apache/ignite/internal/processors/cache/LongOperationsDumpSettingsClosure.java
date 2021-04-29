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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Closure that is sent to the node in order to change
 * "Long operations dump timeout" parameter and also reschedule the task for
 * dumping long operations.
 */
public class LongOperationsDumpSettingsClosure implements IgniteRunnable {
    /** Serialization ID. */
    private static final long serialVersionUID = 0L;

    /** Long operations dump timeout. */
    private final long longOpsDumpTimeout;

    /** Auto-inject Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /**
     * Constructor.
     *
     * @param longOpsDumpTimeout Long operations dump timeout.
     */
    public LongOperationsDumpSettingsClosure(long longOpsDumpTimeout) {
        this.longOpsDumpTimeout = longOpsDumpTimeout;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        ignite.context().cache().context().tm().longOperationsDumpTimeout(longOpsDumpTimeout);
    }
}
