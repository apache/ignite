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

import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processor.security.AbstractPermissionTest;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.plugin.security.SecurityPermission;

import static java.util.Collections.singletonList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Test task execute permission for Executor Service on Client node.
 */
public class ClientNodeExecuteTaskPermissionTest extends AbstractPermissionTest {
    /** Allowed task. */
    private static final IgniteCallable<String> ALLOWED = () -> "RESULT";

    /** Forbidden task. */
    private static final IgniteCallable<String> FORBIDDEN = () -> {
        fail("Should not be invoked.");

        return null;
    };

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        Ignite ignite = startGrid("client",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED.getClass().getName(), SecurityPermission.TASK_EXECUTE)
                .appendTaskPermissions(FORBIDDEN.getClass().getName(), EMPTY_PERMS)
                .build(), isClient()
        );

        assertThat(ignite.executorService().submit(ALLOWED).get(), is("RESULT"));
        forbiddenRun(() -> ignite.executorService().submit(FORBIDDEN));

        Future<String> f = ignite.executorService().invokeAll(singletonList(ALLOWED))
            .stream().findFirst().get();

        assertThat(f.get(), is("RESULT"));
        forbiddenRun(() -> ignite.executorService().invokeAll(singletonList(FORBIDDEN)));

        assertThat(ignite.executorService().invokeAny(singletonList(ALLOWED)), is("RESULT"));
        forbiddenRun(() -> ignite.executorService().invokeAny(singletonList(FORBIDDEN)));

    }
}