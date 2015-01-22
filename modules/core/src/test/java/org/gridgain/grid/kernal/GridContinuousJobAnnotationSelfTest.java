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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Test for various job callback annotations.
 */
@GridCommonTest(group = "Kernal Self")
public class GridContinuousJobAnnotationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicBoolean fail = new AtomicBoolean();

    /** */
    private static final AtomicInteger afterSendCnt = new AtomicInteger();

    /** */
    private static final AtomicInteger beforeFailoverCnt = new AtomicInteger();

    /** */
    private static final AtomicReference<Exception> err = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setMarshalLocalJobs(false);

        return c;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testJobAnnotation() throws Exception {
        testContinuousJobAnnotation(TestJob.class);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testJobChildAnnotation() throws Exception {
        testContinuousJobAnnotation(TestJobChild.class);
    }

    /**
     * @param jobCls Job class.
     * @throws Exception If test failed.
     */
    public void testContinuousJobAnnotation(Class<?> jobCls) throws Exception {
        try {
            Ignite ignite = startGrid(0);
            startGrid(1);

            fail.set(true);

            ignite.compute().execute(TestTask.class, jobCls);

            Exception e = err.get();

            if (e != null)
                throw e;
        }
        finally {
            stopGrid(0);
            stopGrid(1);
        }

        assertEquals(2, afterSendCnt.getAndSet(0));
        assertEquals(1, beforeFailoverCnt.getAndSet(0));
    }

    /** */
    @SuppressWarnings({"PublicInnerClass", "unused"})
    public static class TestTask implements ComputeTask<Object, Object> {
        /** */
        @IgniteTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws IgniteCheckedException {
            try {
                mapper.send(((Class<ComputeJob>)arg).newInstance());
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Job instantination failed.", e);
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received)
            throws IgniteCheckedException {
            if (res.getException() != null) {
                if (res.getException() instanceof ComputeUserUndeclaredException)
                    throw new IgniteCheckedException("Job threw unexpected exception.", res.getException());

                return ComputeJobResultPolicy.FAILOVER;
            }

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            assert results.size() == 1 : "Unexpected result count: " + results.size();

            return null;
        }
    }

    /**
     *
     */
    private static class TestJob extends ComputeJobAdapter {
        /** */
        private boolean flag = true;

        /** */
        TestJob() {
            X.println("Constructing TestJob [this=" + this + ", identity=" + System.identityHashCode(this) + "]");
        }


        /** */
        @ComputeJobAfterSend
        private void afterSend() {
            X.println("AfterSend start TestJob [this=" + this + ", identity=" + System.identityHashCode(this) +
                ", flag=" + flag + "]");

            afterSendCnt.incrementAndGet();

            flag = false;

            X.println("AfterSend end TestJob [this=" + this + ", identity=" + System.identityHashCode(this) +
                ", flag=" + flag + "]");
        }

        /** */
        @ComputeJobBeforeFailover
        private void beforeFailover() {
            X.println("BeforeFailover start TestJob [this=" + this + ", identity=" + System.identityHashCode(this) +
                ", flag=" + flag + "]");

            beforeFailoverCnt.incrementAndGet();

            flag = true;

            X.println("BeforeFailover end TestJob [this=" + this + ", identity=" + System.identityHashCode(this) +
                ", flag=" + flag + "]");
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws IgniteCheckedException {
            X.println("Execute TestJob [this=" + this + ", identity=" + System.identityHashCode(this) +
                ", flag=" + flag + "]");

            if (!flag) {
                String msg = "Flag is false on execute [this=" + this + ", identity=" + System.identityHashCode(this) +
                    ", flag=" + flag + "]";

                X.println(msg);

                err.compareAndSet(null, new Exception(msg));
            }

            if (fail.get()) {
                fail.set(false);

                throw new IgniteCheckedException("Expected test exception.");
            }

            return null;
        }
    }

    /**
     *
     */
    private static class TestJobChild extends TestJob {
        /**
         * Required for reflectional creation.
         */
        TestJobChild() {
            // No-op.
        }
    }
}
