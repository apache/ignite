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

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;

import static java.util.Collections.singletonList;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;

/**
 * Test task execute permission for Executor Service on Client node.
 */
public class ClientNodeTaskExecutePermissionForExecutorServiceTest extends AbstractTaskExecutePermissionTest {
    /**
     * @throws Exception If failed.
     */
    public void testExecute() throws Exception {
        Ignite ignite = startGrid("client",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED_CALLABLE.getClass().getName(), TASK_EXECUTE)
                .appendTaskPermissions(FORBIDDEN_CALLABLE.getClass().getName(), EMPTY_PERMS)
                .build(), isClient()
        );

        allowRun(() -> ignite.executorService().submit(ALLOWED_CALLABLE).get());
        forbiddenRun(() -> ignite.executorService().submit(FORBIDDEN_CALLABLE).get());

        allowRun(()->ignite.executorService().invokeAll(singletonList(ALLOWED_CALLABLE)));
        forbiddenRun(() -> ignite.executorService().invokeAll(singletonList(FORBIDDEN_CALLABLE)));

        allowRun(()->ignite.executorService().invokeAny(singletonList(ALLOWED_CALLABLE)));
        forbiddenRun(() -> ignite.executorService().invokeAny(singletonList(FORBIDDEN_CALLABLE)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllowedCancel() throws Exception {
        Ignite ignite = startGrid("client_allowed_cancel",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED_CALLABLE.getClass().getName(), TASK_EXECUTE, TASK_CANCEL)
                .build(), isClient()
        );

        Future<Object> f = ignite.executorService().submit(ALLOWED_CALLABLE);

        f.cancel(true);

        forbiddenRun(f::get, CancellationException.class);
    }

    /**
     * @throws Exception If failed.
     */
    public void testForbiddenCancel() throws Exception {
        Ignite ignite = startGrid("client_forbidden_cancel",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED_CALLABLE.getClass().getName(), TASK_EXECUTE)
                .build(), isClient()
        );

        Future<Object> f = ignite.executorService().submit(ALLOWED_CALLABLE);

        forbiddenRun(() -> f.cancel(true));
    }
}