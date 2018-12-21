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

package org.apache.ignite.internal.processor.security.compute;

import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 * Test task execute permission for compute broadcast.
 */
public class TaskExecutePermissionForDistributedClosureTest extends AbstractTaskExecutePermissionTest {
    /** Test callable. */
    protected static final IgniteCallable<Object> TEST_CALLABLE = () -> {
        IS_EXECUTED.set(true);

        return null;
    };

    /** Test runnable. */
    private static final IgniteRunnable TEST_RUNNABLE = () -> IS_EXECUTED.set(true);

    /** Test closure. */
    private static final IgniteClosure<Object, Object> TEST_CLOSURE = a -> {
        IS_EXECUTED.set(true);

        return null;
    };

    /** {@inheritDoc} */
    @Override protected Supplier<FutureAdapter> cancelSupplier(Ignite node) {
        return () -> new FutureAdapter(node.compute().broadcastAsync(TEST_CALLABLE));
    }

    /** {@inheritDoc} */
    @Override protected TestRunnable[] runnables(Ignite node) {
        return new TestRunnable[]{
            () -> node.compute().broadcast(TEST_CALLABLE),
            () -> node.compute().broadcastAsync(TEST_CALLABLE).get(),
            () -> node.compute().call(TEST_CALLABLE),
            () -> node.compute().callAsync(TEST_CALLABLE).get(),
            () -> node.compute().run(TEST_RUNNABLE),
            () -> node.compute().runAsync(TEST_RUNNABLE).get(),
            () -> node.compute().apply(TEST_CLOSURE, new Object()),
            () -> node.compute().applyAsync(TEST_CLOSURE, new Object()).get()
        };
    }

    /** {@inheritDoc} */
    @Override protected SecurityPermissionSet permissions(SecurityPermission... perms) {
        return builder().defaultAllowAll(true)
            .appendTaskPermissions(TEST_CALLABLE.getClass().getName(), perms)
            .appendTaskPermissions(TEST_RUNNABLE.getClass().getName(), perms)
            .appendTaskPermissions(TEST_CLOSURE.getClass().getName(), perms)
            .build();
    }
}
