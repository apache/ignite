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

import javax.cache.Cache;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ScanQuery;
import org.junit.Test;

/**
 * Checks that a remote filter and transformer of {@code ContinuousQueryWithTransformer} run on a remote node inside the
 * Ignite Sandbox.
 */
public class ContinuousQueryWithTransformerSandboxTest extends AbstractContinuousQuerySandboxTest {
    /** */
    @Test
    public void testInitialQuery() {
        checkContinuousQuery(() -> {
            ContinuousQueryWithTransformer<Integer, Integer, Integer> q = new ContinuousQueryWithTransformer<>();

            q.setInitialQuery(new ScanQuery<>(INIT_QRY_FILTER));
            q.setRemoteTransformerFactory(() -> Cache.Entry::getValue);
            q.setLocalListener(e -> {/* No-op. */});

            return q;
        }, true);
    }

    /** */
    @Test
    public void testRemoteFilterFactory() {
        checkContinuousQuery(() -> {
            ContinuousQueryWithTransformer<Integer, Integer, Integer> q = new ContinuousQueryWithTransformer<>();

            q.setRemoteFilterFactory(() -> RMT_FILTER);
            q.setRemoteTransformerFactory(() -> Cache.Entry::getValue);

            return q;
        }, false);
    }

    /** */
    @Test
    public void testTransformerFactory() {
        checkContinuousQuery(() -> {
            ContinuousQueryWithTransformer<Integer, Integer, Integer> q = new ContinuousQueryWithTransformer<>();

            q.setRemoteTransformerFactory(() -> e -> {
                CONTROL_ACTION_RUNNER.run();

                return e.getValue();
            });
            q.setLocalListener(e -> {/* No-op. */});

            return q;
        }, false);
    }
}
