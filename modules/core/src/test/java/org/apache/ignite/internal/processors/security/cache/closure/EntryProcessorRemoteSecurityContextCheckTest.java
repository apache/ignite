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

package org.apache.ignite.internal.processors.security.cache.closure;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singleton;
import static org.apache.ignite.Ignition.localIgnite;

/**
 * Testing operation security context when EntryProcessor closure is executed on remote node.
 * <p>
 * The initiator node broadcasts a task to 'run' node that starts EntryProcessor closure. That closure is executed on
 * 'check' node and broadcasts a task to 'endpoint' nodes. On every step, it is performed verification that operation
 * security context is the initiator context.
 */
@RunWith(JUnit4.class)
public class EntryProcessorRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startGridAllowAll(SRV_CHECK);

        startGridAllowAll(SRV_ENDPOINT);

        startClientAllowAll(CLNT_ENDPOINT);

        G.allGrids().get(0).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void test() {
        runAndCheck(operations());
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToRun() {
        return Collections.singletonList(SRV_RUN);
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToCheck() {
        return Collections.singletonList(SRV_CHECK);
    }

    /**
     * @return Stream of runnables to call invoke methods.
     */
    private Stream<IgniteRunnable> operations() {
        final Integer key = primaryKey(grid(SRV_CHECK));

        return Stream.of(
            () -> localIgnite().<Integer, Integer>cache(CACHE_NAME).invoke(key, operationCheck()),

            () -> localIgnite().<Integer, Integer>cache(CACHE_NAME).invokeAll(singleton(key), operationCheck()),

            () -> localIgnite().<Integer, Integer>cache(CACHE_NAME).invokeAsync(key, operationCheck()).get(),

            () -> localIgnite().<Integer, Integer>cache(CACHE_NAME)
                .invokeAllAsync(singleton(key), operationCheck()).get()
        );
    }
}
