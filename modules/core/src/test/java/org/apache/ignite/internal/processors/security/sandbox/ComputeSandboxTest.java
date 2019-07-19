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

import java.security.AccessControlException;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.security.impl.FutureAdapter;
import org.apache.ignite.internal.processors.security.impl.TestComputeTask;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** . */
public class ComputeSandboxTest extends AbstractSandboxTest {
    /** . */
    private static final TestComputeTask COMPUTE_TASK = new TestComputeTask(START_THREAD_RUNNABLE);

    /** . */
    private static final IgniteCallable<Object> CALLABLE = () -> {
        START_THREAD_RUNNABLE.run();

        return null;
    };

    /** . */
    private static final IgniteClosure<Object, Object> CLOSURE = a -> {
        START_THREAD_RUNNABLE.run();

        return null;
    };

    /** . */
    @Test
    public void test() throws Exception {
        prepareCluster();

        Ignite clntAllowed = grid(CLNT_ALLOWED);

        Ignite clntFrobidden = grid(CLNT_FORBIDDEN);

        computeOperations(clntAllowed).forEach(this::runOperation);
        computeOperations(clntFrobidden).forEach(op -> assertThrowsWithCause(op, AccessControlException.class));

        executorServiceOperations(clntAllowed).forEach(this::runOperation);
        executorServiceOperations(clntFrobidden).forEach(this::runFailedOperation);
    }

    /**
     * @return Stream of Compute operations to test.
     */
    private Stream<GridTestUtils.RunnableX> computeOperations(Ignite node) {
        return Stream.of(
            () -> node.compute().execute(COMPUTE_TASK, 0),
            () -> node.compute().broadcast(CALLABLE),
            () -> node.compute().call(CALLABLE),
            () -> node.compute().run(START_THREAD_RUNNABLE),
            () -> node.compute().apply(CLOSURE, new Object()),

            () -> new FutureAdapter<>(node.compute().executeAsync(COMPUTE_TASK, 0)).get(),
            () -> new FutureAdapter<>(node.compute().broadcastAsync(CALLABLE)).get(),
            () -> new FutureAdapter<>(node.compute().callAsync(CALLABLE)).get(),
            () -> new FutureAdapter<>(node.compute().runAsync(START_THREAD_RUNNABLE)).get(),
            () -> new FutureAdapter<>(node.compute().applyAsync(CLOSURE, new Object())).get()
        );
    }

    /**
     * @return Stream of ExecutorService operations to test.
     */
    private Stream<GridTestUtils.RunnableX> executorServiceOperations(Ignite node) {
        return Stream.of(
            () -> node.executorService().invokeAll(singletonList(CALLABLE)),
            () -> node.executorService().invokeAny(singletonList(CALLABLE)),
            () -> node.executorService().submit(CALLABLE).get()
        );
    }

    /** . */
    private void runFailedOperation(Runnable r) {
        IS_STARTED.set(false);

        try {
            r.run();
        }
        catch (Exception e) {
            //ignore.
        }

        assertFalse(IS_STARTED.get());
    }
}
