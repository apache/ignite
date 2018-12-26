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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processor.security.AbstractSecurityTest;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;

/**
 * Abstract class for task execute permission tests.
 */
@RunWith(JUnit4.class)
public abstract class AbstractTaskExecutePermissionTest extends AbstractSecurityTest {
    /** Flag that shows task was executed. */
    protected static final AtomicBoolean IS_EXECUTED = new AtomicBoolean(false);

    /** Server allowed all task permissions. */
    protected Ignite srvAllowed;

    /** Server forbidden all task permissions. */
    protected Ignite srvForbidden;

    /** Server forbidden cancel task permission. */
    protected Ignite srvForbiddenCancel;

    /** Client allowed all task permissions. */
    protected Ignite clntAllowed;

    /** Client forbidden all task permissions. */
    protected Ignite clntForbidden;

    /** Client forbidden cancel task permission. */
    protected Ignite clntForbiddenCancel;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srvAllowed = startGrid("srv_allowed", permissions(TASK_EXECUTE, TASK_CANCEL));

        srvForbidden = startGrid("srv_forbidden", permissions(EMPTY_PERMS));

        srvForbiddenCancel = startGrid("srv_forbidden_cnl", permissions(TASK_EXECUTE));

        clntAllowed = startGrid("clnt_allowed", permissions(TASK_EXECUTE, TASK_CANCEL), true);

        clntForbidden = startGrid("srv_forbidden", permissions(EMPTY_PERMS), true);

        clntForbiddenCancel = startGrid("clnt_forbidden_cnl", permissions(TASK_EXECUTE), true);

        srvAllowed.cluster().active(true);
    }

    /**
     *
     */
    @Test
    public void test() {
        for (TestRunnable r : runnablesForNodes(srvAllowed, clntAllowed))
            allowRun(r);

        for (TestRunnable r : runnablesForNodes(srvForbidden, clntForbidden))
            forbiddenRun(r);

        allowedCancel(cancelSupplier(srvAllowed));
        allowedCancel(cancelSupplier(clntAllowed));

        forbiddenCancel(cancelSupplier(srvForbiddenCancel));
        forbiddenCancel(cancelSupplier(clntForbiddenCancel));
    }

    /**
     * @param perms Permissions.
     */
    protected abstract SecurityPermissionSet permissions(SecurityPermission... perms);

    /**
     * @param node Node.
     */
    protected abstract Supplier<FutureAdapter> cancelSupplier(Ignite node);

    /**
     * @param node Node.
     * @return Array of runnable that invoke set of compute methods on passed node.
     */
    protected abstract TestRunnable[] runnables(Ignite node);

    /**
     * @param nodes Array of nodes.
     */
    private Collection<TestRunnable> runnablesForNodes(Ignite... nodes) {
        List<TestRunnable> res = new ArrayList<>();

        for (Ignite node : nodes)
            res.addAll(Arrays.asList(runnables(node)));

        return res;
    }

    /**
     * @param r TestRunnable.
     */
    private void allowRun(TestRunnable r) {
        IS_EXECUTED.set(false);

        try {
            r.run();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertTrue(IS_EXECUTED.get());
    }

    /**
     * @param s Supplier.
     */
    private void forbiddenCancel(Supplier<FutureAdapter> s) {
        FutureAdapter f = s.get();

        forbiddenRun(f::cancel);
    }

    /**
     * @param s Supplier.
     */
    private void allowedCancel(Supplier<FutureAdapter> s) {
        FutureAdapter f = s.get();

        f.cancel();

        forbiddenRun(f::get, CancellationException.class, IgniteFutureCancelledException.class);
    }

    /**
     *
     */
    static class FutureAdapter {
        /** Ignite future. */
        private final IgniteFuture igniteFut;

        /** Future. */
        private final Future fut;

        /**
         * @param igniteFut Ignite future.
         */
        public FutureAdapter(IgniteFuture igniteFut) {
            this.igniteFut = igniteFut;
            fut = null;
        }

        /**
         * @param fut Future.
         */
        public FutureAdapter(Future fut) {
            this.fut = fut;
            igniteFut = null;
        }


        /**
         *
         */
        public void cancel() {
            assert igniteFut != null || fut != null;

            if (igniteFut != null)
                igniteFut.cancel();
            else
                fut.cancel(true);
        }

        /**
         *
         */
        public Object get() throws ExecutionException, InterruptedException {
            assert igniteFut != null || fut != null;

            return igniteFut != null ? igniteFut.get() : fut.get();
        }
    }
}
