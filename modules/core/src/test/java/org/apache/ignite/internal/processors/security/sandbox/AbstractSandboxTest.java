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
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.IgniteSecurityManager;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static sun.security.util.SecurityConstants.MODIFY_THREADGROUP_PERMISSION;
import static sun.security.util.SecurityConstants.MODIFY_THREAD_PERMISSION;

/** */
public abstract class AbstractSandboxTest extends AbstractSecurityTest {
    /** Flag that shows thread was started. */
    protected static final AtomicBoolean IS_STARTED = new AtomicBoolean(false);

    /** */
    protected static final String TEST_CACHE = "test_cache";

    /** */
    private static final ReentrantLock LOCK = new ReentrantLock();

    /** */
    private static final int LOCK_TIMEOUT = 500;

    /** */
    private static boolean setupSM;

    /** Sever node name. */
    protected static final String SRV = "srv";

    /** Client node that can start a new thread. */
    protected static final String CLNT_ALLOWED_THREAD_START = "clnt_allowed";

    /** Client node that cannot start a new thread. */
    protected static final String CLNT_FORBIDDEN_THREAD_START = "clnt_forbidden";

    /** */
    public static final IgniteRunnable START_THREAD_RUNNABLE = () -> {
        LOCK.lock();

        try {
            new Thread(() -> IS_STARTED.set(true)).start();

            while (!IS_STARTED.get())
                TimeUnit.MILLISECONDS.sleep(100);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        finally {
            LOCK.unlock();
        }
    };

    /** */
    @BeforeClass
    public static void setup() {
        if (System.getSecurityManager() == null) {
            Policy.setPolicy(new Policy() {
                @Override public PermissionCollection getPermissions(CodeSource cs) {
                    Permissions res = new Permissions();

                    res.add(new AllPermission());

                    return res;
                }
            });

            System.setSecurityManager(new IgniteSecurityManager());

            setupSM = true;
        }
    }

    /** */
    @AfterClass
    public static void tearDown() {
        if (setupSM) {
            System.setSecurityManager(null);
            Policy.setPolicy(null);
        }
    }

    /** */
    protected void prepareCluster() throws Exception {
        Ignite srv = startGrid(SRV, ALLOW_ALL, false);

        startGrid(CLNT_ALLOWED_THREAD_START, ALLOW_ALL,
            TestPermissionsBuilder.create()
                .add(MODIFY_THREAD_PERMISSION)
                .add(MODIFY_THREADGROUP_PERMISSION).get(), true);

        startGrid(CLNT_FORBIDDEN_THREAD_START, ALLOW_ALL, true);

        srv.cluster().active(true);
    }

    /** */
    protected void runOperation(Runnable r) {
        IS_STARTED.set(false);

        r.run();

        waitStarted();

        assertTrue(IS_STARTED.get());
    }

    /** */
    protected void runOperation(Supplier<Object> s) {
        runOperation((Runnable)s::get);
    }

    /** */
    protected void runForbiddenOperation(GridTestUtils.RunnableX runnable, Class<? extends Throwable> cls) {
        IS_STARTED.set(false);

        assertThrowsWithCause(runnable, cls);

        assertFalse(IS_STARTED.get());
    }

    /** */
    protected void runForbiddenOperation(Supplier<Object> s) {
        runForbiddenOperation(
            () -> {
                Object res = s.get();

                if (res instanceof Exception)
                    throw (Exception)res;
            },
            AccessControlException.class
        );
    }

    /** */
    private void waitStarted() {
        boolean isLocked = false;

        try {
            isLocked = LOCK.tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (isLocked)
                LOCK.unlock();
        }
    }

    /** */
     protected static class TestPermissionsBuilder {
        /** */
        public static TestPermissionsBuilder create() {
            return new TestPermissionsBuilder();
        }

        /** */
        public static Permissions createAllowAll() {
            Permissions res = new Permissions();

            res.add(new AllPermission());

            return res;
        }

        /** */
        private final Permissions perms;

        /** */
        private TestPermissionsBuilder() {
            perms = new Permissions();
        }

        /** */
        public TestPermissionsBuilder add(Permission perm) {
            perms.add(perm);

            return this;
        }

        /** */
        public Permissions get() {
            return perms;
        }
    }
}
