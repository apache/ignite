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

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteRunnable;

import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;

/**
 * Test task execute permission for compute broadcast.
 */
public class TaskExecutePermissionForDistributedClosureTest extends AbstractTaskExecutePermissionTest {
    /** Allowed runnable. */
    private static final IgniteRunnable ALLOWED_RUNNABLE = () -> JINGLE_BELL.set(true);

    /** Forbidden runnable. */
    private static final IgniteRunnable FORBIDDEN_RUNNABLE = () -> fail("Should not be invoked.");

    /** Allow closure. */
    private static final IgniteClosure<Object, Object> ALLOW_CLOSURE = a -> {
        JINGLE_BELL.set(true);

        return null;
    };

    /** Forbidden closure. */
    private static final IgniteClosure<Object, Object> FORBIDDEN_CLOSURE = a -> {
        fail("Should not be invoked.");

        return null;
    };


    /** {@inheritDoc} */
    @Override protected void testExecute(boolean isClient) throws Exception {
        Ignite node = startGrid(loginPrefix(isClient) + "_node",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED_CALLABLE.getClass().getName(), TASK_EXECUTE)
                .appendTaskPermissions(FORBIDDEN_CALLABLE.getClass().getName(), EMPTY_PERMS)
                .appendTaskPermissions(ALLOWED_RUNNABLE.getClass().getName(), TASK_EXECUTE)
                .appendTaskPermissions(FORBIDDEN_RUNNABLE.getClass().getName(), EMPTY_PERMS)
                .appendTaskPermissions(ALLOW_CLOSURE.getClass().getName(), TASK_EXECUTE)
                .appendTaskPermissions(FORBIDDEN_CLOSURE.getClass().getName(), EMPTY_PERMS)
                .build(), isClient
        );

        allowRun(() -> node.compute().broadcast(ALLOWED_CALLABLE));
        forbiddenRun(() -> node.compute().broadcast(FORBIDDEN_CALLABLE));

        allowRun(() -> node.compute().broadcastAsync(ALLOWED_CALLABLE).get());
        forbiddenRun(() -> node.compute().broadcastAsync(FORBIDDEN_CALLABLE).get());

        allowRun(() -> node.compute().call(ALLOWED_CALLABLE));
        forbiddenRun(() -> node.compute().call(FORBIDDEN_CALLABLE));

        allowRun(() -> node.compute().callAsync(ALLOWED_CALLABLE).get());
        forbiddenRun(() -> node.compute().callAsync(FORBIDDEN_CALLABLE).get());

        allowRun(() -> node.compute().run(ALLOWED_RUNNABLE));
        forbiddenRun(() -> node.compute().run(FORBIDDEN_RUNNABLE));

        allowRun(() -> node.compute().runAsync(ALLOWED_RUNNABLE).get());
        forbiddenRun(() -> node.compute().runAsync(FORBIDDEN_RUNNABLE).get());

        Object arg = new Object();

        allowRun(() -> node.compute().apply(ALLOW_CLOSURE, arg));
        forbiddenRun(() -> node.compute().apply(FORBIDDEN_CLOSURE, arg));

        allowRun(() -> node.compute().applyAsync(ALLOW_CLOSURE, arg).get());
        forbiddenRun(() -> node.compute().applyAsync(FORBIDDEN_CLOSURE, arg).get());
    }

    /** {@inheritDoc} */
    @Override protected void testAllowedCancel(boolean isClient) throws Exception {
        Ignite node = startGrid(loginPrefix(isClient) + "_allowed_cancel",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED_CALLABLE.getClass().getName(), TASK_EXECUTE, TASK_CANCEL)
                .build(), isClient
        );

        IgniteFuture<Collection<Object>> f = node.compute().broadcastAsync(ALLOWED_CALLABLE);

        f.cancel();

        forbiddenRun(f::get, IgniteFutureCancelledException.class);
    }


    /** {@inheritDoc} */
    @Override protected void testForbiddenCancel(boolean isClient) throws Exception {
        Ignite node = startGrid("client_forbidden_cancel",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED_CALLABLE.getClass().getName(), TASK_EXECUTE)
                .build(), isClient
        );

        IgniteFuture<Collection<Object>> f = node.compute().broadcastAsync(ALLOWED_CALLABLE);

        forbiddenRun(f::cancel);
    }
}
