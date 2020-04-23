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

package org.apache.ignite.internal.processors.service.inner;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * Test service with long initialization to delay processing of exchange queue.
 */
public class LongInitializedTestService implements Service {
    /** Time to delay. */
    private long delay;

    /**
     * @param delay Milliseconds to delay initialization.
     */
    public LongInitializedTestService(long delay) {
        this.delay = delay;
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        U.sleep(delay);
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        // No-op.
    }
}
