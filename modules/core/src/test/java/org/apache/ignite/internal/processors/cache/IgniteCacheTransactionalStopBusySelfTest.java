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

import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;

/**
 * Stopped node when client operations are executing.
 */
public class IgniteCacheTransactionalStopBusySelfTest extends IgniteCacheAbstractStopBusySelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-257");
    }

    /** {@inheritDoc} */
    @Override public void testPut() throws Exception {
        bannedMsg.set(GridNearTxPrepareRequest.class);

        super.testPut();
    }

    /** {@inheritDoc} */
    @Override public void testPutBatch() throws Exception {
        bannedMsg.set(GridNearTxPrepareRequest.class);

        super.testPut();
    }

    /** {@inheritDoc} */
    @Override public void testPutAsync() throws Exception {
        bannedMsg.set(GridNearTxPrepareRequest.class);

        super.testPut();
    }

    /** {@inheritDoc} */
    @Override public void testRemove() throws Exception {
        bannedMsg.set(GridNearTxPrepareRequest.class);

        super.testPut();
    }
}