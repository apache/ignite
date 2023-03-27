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

package org.apache.ignite.internal.processors.security.sandbox;

import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.junit.Test;

/**
 * Checks that a remote filter of {@code ContinuousQueries} runs on a remote node inside the Ignite Sandbox.
 */
public class ContinuousQuerySandboxTest extends AbstractContinuousQuerySandboxTest {
    /** */
    @Test
    public void testInitialQueryFilter() {
        checkContinuousQuery(() -> {
            ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();

            cq.setInitialQuery(new ScanQuery<>(INIT_QRY_FILTER));
            cq.setLocalListener(e -> {/* No-op. */});

            return cq;
        }, true);
    }

    /** */
    @Test
    public void testRemoteFilterFactory() {
        checkContinuousQuery(() -> {
            ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();

            cq.setRemoteFilterFactory(() -> RMT_FILTER);

            return cq;
        }, false);
    }

    /** */
    @Test
    public void testRemoteFilter() {
        checkContinuousQuery(() -> {
            ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();

            cq.setRemoteFilter(RMT_FILTER);

            return cq;
        }, false);
    }
}
