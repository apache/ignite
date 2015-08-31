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

package org.apache.ignite.session;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests waiting for session attributes.
 */
@GridCommonTest(group = "Task Session")
@SuppressWarnings({"PublicInnerClass"})
public class GridSessionWaitAttributeSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int ATTR_NUM = 100;

    /** */
    private static final int JOB_NUM = 10;

    /** */
    private static final long WAIT_TIMEOUT = 20000;

    /** */
    private enum WaitAttributeType {
        /** waitForAttribute(Serializable key). */
        WAIT_FOR_ATTRIBUTE_KEY,

        /** waitForAttribute(Serializable key, Serializable val). */
        WAIT_FOR_ATTRIBUTE_KEY_VAL,

        /** waitForAttribute(Serializable key, long timeout). */
        WAIT_FOR_ATTRIBUTE_KEY_TIMEOUT,

        /** waitForAttribute(Serializable key, Serializable val, long timeout). */
        WAIT_FOR_ATTRIBUTE_KEY_VAL_TIMEOUT,

        /** waitForAttributes(Collection<? extends Serializable> keys). */
        WAIT_FOR_ATTRIBUTES_KEYS,

        /** waitForAttributes(Map<? extends Serializable, ? extends Serializable> attrs). */
        WAIT_FOR_ATTRIBUTES_ATTRS,

        /** waitForAttributes(Collection<? extends Serializable> keys, long timeout). */
        WAIT_FOR_ATTRIBUTES_KEYS_TIMEOUT,

        /** waitForAttributes(Map<? extends Serializable, ? extends Serializable> attrs, long timeout). */
        WAIT_FOR_ATTRIBUTES_ATTRS_TIMEOUT
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        c.setPublicThreadPoolSize(JOB_NUM * 2);

        return c;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid(1);
        stopGrid(2);
    }

    /**
     * @param prefix Prefix.
     * @param mtd Method.
     * @param i Index.
     * @return Session attribute key.
     */
    private static String createKey(String prefix, Enum mtd, int i) {
        assert prefix != null;
        assert mtd != null;

        return prefix + "test.key." + mtd.name() + '.' + i;
    }

    /**
     * @param prefix Prefix.
     * @param mtd Method.
     * @param i Index.
     * @return Session attribute value.
     */
    private static String createValue(String prefix, Enum mtd, int i) {
        assert prefix != null;
        assert mtd != null;

        return prefix + "test.value." + mtd.name() + '.' + i;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitAttribute() throws Exception {
        checkWaitAttributeMethod(WaitAttributeType.WAIT_FOR_ATTRIBUTE_KEY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitAttributeWithTimeout() throws Exception {
        checkWaitAttributeMethod(WaitAttributeType.WAIT_FOR_ATTRIBUTE_KEY_TIMEOUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitAttributeValue() throws Exception {
        checkWaitAttributeMethod(WaitAttributeType.WAIT_FOR_ATTRIBUTE_KEY_VAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitAttributeValueWithTimeout() throws Exception {
        checkWaitAttributeMethod(WaitAttributeType.WAIT_FOR_ATTRIBUTE_KEY_VAL_TIMEOUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitAttributeValues() throws Exception {
        checkWaitAttributeMethod(WaitAttributeType.WAIT_FOR_ATTRIBUTES_ATTRS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitAttributeValuesWithTimeout() throws Exception {
        checkWaitAttributeMethod(WaitAttributeType.WAIT_FOR_ATTRIBUTES_ATTRS_TIMEOUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitAttributes() throws Exception {
        checkWaitAttributeMethod(WaitAttributeType.WAIT_FOR_ATTRIBUTES_KEYS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitAttributesWithTimeout() throws Exception {
        checkWaitAttributeMethod(WaitAttributeType.WAIT_FOR_ATTRIBUTES_KEYS_TIMEOUT);
    }

    /**
     * @param type Type.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkWaitAttributeMethod(WaitAttributeType type) throws Exception {
        assert type != null;

        Ignite ignite1 = G.ignite(getTestGridName() + '1');
        Ignite ignite2 = G.ignite(getTestGridName() + '2');

        assert ignite1 != null;
        assert ignite2 != null;

        ignite1.compute().localDeployTask(TestSessionTask.class, TestSessionTask.class.getClassLoader());

        IgniteCompute comp = ignite1.compute().withAsync();

        comp.execute(TestSessionTask.class.getName(), type);

        ComputeTaskFuture<?> fut = comp.future();

        fut.getTaskSession().mapFuture().get();

        ComputeTaskSession ses = fut.getTaskSession();

        info("Task job siblings [size=" + ses.getJobSiblings().size() + ", siblings=" + ses.getJobSiblings() + ']');

        for (int i = 0; i < ATTR_NUM; i++) {
            String key = createKey("fut", type, i);
            String val = createValue("fut", type, i);

            ses.setAttribute(key, val);
        }

        // Check all job attributes.
        for (ComputeJobSibling sibling : ses.getJobSiblings()) {
            info("Checking session attributes for sibling: " + sibling);

            checkSessionAttributes(ses, sibling.getJobId().toString(), type);
        }

        // Check that fut attributes have been set.
        checkSessionAttributes(ses, "fut", type);

        // Signal finish.
        ses.setAttribute("done", true);

        fut.get();
    }

    /**
     * @param ses Session.
     * @param prefix Prefix.
     * @param type Type.
     * @throws IgniteCheckedException If failed.
     */
    private static void checkSessionAttributes(ComputeTaskSession ses, String prefix, WaitAttributeType type) {
        assert ses != null;
        assert type != null;

        try {
            switch (type) {
                case WAIT_FOR_ATTRIBUTE_KEY: {
                    for (int i = 0; i < ATTR_NUM; i++) {
                        String key = createKey(prefix, type, i);
                        String val = createValue(prefix, type, i);

                        Serializable obj = ses.waitForAttribute(key, 0);

                        assert obj != null :
                            "Failed to wait for attribute [key=" + key + ", val=" + val + ", receivedVal=" + obj + ']';
                        assert val.equals(obj) :
                            "Failed to wait for attribute [key=" + key + ", val=" + val + ", receivedVal=" + obj + ']';

                        //System.out.println(Thread.currentThread().getName() + ":: Waited for attribute [key=" + key + ", val=" + obj + ", ses=" + ses + ']');
                    }

                    break;
                }

                case WAIT_FOR_ATTRIBUTE_KEY_TIMEOUT: {
                    for (int i = 0; i < ATTR_NUM; i++) {
                        String key = createKey(prefix, type, i);
                        String val = createValue(prefix, type, i);

                        Serializable obj = ses.waitForAttribute(key, WAIT_TIMEOUT);

                        assert obj != null :
                            "Failed to wait for attribute [key=" + key + ", val=" + val + ", receivedVal=" + obj + ']';
                        assert val.equals(obj) :
                            "Failed to wait for attribute [key=" + key + ", val=" + val + ", receivedVal=" + obj + ']';
                    }

                    break;
                }

                case WAIT_FOR_ATTRIBUTE_KEY_VAL: {
                    for (int i = 0; i < ATTR_NUM; i++) {
                        String key = createKey(prefix, type, i);
                        String val = createValue(prefix, type, i);

                        boolean attr = ses.waitForAttribute(key, val, 0);

                        assert attr :
                            "Failed to wait for attribute [key=" + key + ", val=" + val + ']';
                    }

                    break;
                }

                case WAIT_FOR_ATTRIBUTE_KEY_VAL_TIMEOUT: {
                    for (int i = 0; i < ATTR_NUM; i++) {
                        String key = createKey(prefix, type, i);
                        String val = createValue(prefix, type, i);

                        boolean attr = ses.waitForAttribute(key, val, WAIT_TIMEOUT);

                        assert attr :
                            "Failed to wait for attribute [key=" + key + ", val=" + val + ']';
                    }

                    break;
                }

                case WAIT_FOR_ATTRIBUTES_ATTRS: {
                    Map<Object, Object> map = new HashMap<>();

                    for (int i = 0; i < ATTR_NUM; i++)
                        map.put(createKey(prefix, type, i), createValue(prefix, type, i));

                    boolean attrs = ses.waitForAttributes(map, 0);

                    assert attrs :
                        "Failed to wait for attribute [attrs=" + map + ']';

                    break;
                }

                case WAIT_FOR_ATTRIBUTES_ATTRS_TIMEOUT: {
                    Map<Object, Object> map = new HashMap<>();

                    for (int i = 0; i < ATTR_NUM; i++)
                        map.put(createKey(prefix, type, i), createValue(prefix, type, i));

                    boolean attrs = ses.waitForAttributes(map, WAIT_TIMEOUT);

                    assert attrs :
                        "Failed to wait for attribute [attrs=" + map + ']';

                    break;
                }

                case WAIT_FOR_ATTRIBUTES_KEYS: {
                    Map<Object, Object> map = new HashMap<>();

                    for (int i = 0; i < ATTR_NUM; i++)
                        map.put(createKey(prefix, type, i), createValue(prefix, type, i));

                    Map<?, ?> res = ses.waitForAttributes(map.keySet(), 0);

                    assert res != null : "Failed to wait for attribute [keys=" + map.keySet() + ']';

                    for (Map.Entry<Object, Object> entry : map.entrySet()) {
                        Object obj = res.get(entry.getKey());

                        assert obj != null : "Failed to get value from result map [key=" + entry.getKey() + ']';
                        assert entry.getValue().equals(obj) : "Fount unexpected value [key=" + entry.getKey()
                            + ", val=" + obj + ", expected=" + entry.getValue();
                    }

                    break;
                }

                case WAIT_FOR_ATTRIBUTES_KEYS_TIMEOUT: {
                    Map<Object, Object> map = new HashMap<>();

                    for (int i = 0; i < ATTR_NUM; i++)
                        map.put(createKey(prefix, type, i), createValue(prefix, type, i));

                    Map<?, ?> res = ses.waitForAttributes(map.keySet(), WAIT_TIMEOUT);

                    assert res != null : "Failed to wait for attribute [keys=" + map.keySet() + ']';

                    for (Map.Entry<Object, Object> entry : map.entrySet()) {
                        Object obj = res.get(entry.getKey());

                        assert obj != null : "Failed to get value from result map [key=" + entry.getKey() + ']';
                        assert entry.getValue().equals(obj) : "Fount unexpected value [key=" + entry.getKey()
                            + ", val=" + obj + ", expected=" + entry.getValue();
                    }

                    break;
                }

                default: {
                    assert false : "Unknown session wait type.";
                }
            }
        }
        catch (InterruptedException e) {
            throw new IgniteException("Got interrupted while waiting for session attributes.", e);
        }
    }

    /** */
    @ComputeTaskSessionFullSupport
    public static class TestSessionTask extends ComputeTaskSplitAdapter<WaitAttributeType, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<TestSessionJob> split(int gridSize, WaitAttributeType type) {
            assert type != null;

            Collection<TestSessionJob> jobs = new ArrayList<>(JOB_NUM);

            for (int i = 0; i < JOB_NUM; i++)
                jobs.add(new TestSessionJob(type));

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /** */
    public static class TestSessionJob extends ComputeJobAdapter {
        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param arg Wait attribute type.
         */
        public TestSessionJob(WaitAttributeType arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            WaitAttributeType m = argument(0);

            checkSessionAttributes(taskSes, "fut", m);

            IgniteUuid jobId = jobCtx.getJobId();

            for (int i = 0; i < ATTR_NUM; i ++) {
                String key = createKey(jobId.toString(), m, i);
                String val = createValue(jobId.toString(), m, i);

                taskSes.setAttribute(key, val);
            }

            // Check that attributes just set are present.
            checkSessionAttributes(taskSes, jobId.toString(), m);

            Collection<ComputeJobSibling> siblings = taskSes.getJobSiblings();

            if (log.isInfoEnabled())
                log.info("Got siblings from job [size=" + siblings.size() + ", siblings=" + siblings + ']');

            // Check attributes from siblings.
            for (ComputeJobSibling sibling : taskSes.getJobSiblings()) {
                if (!sibling.getJobId().equals(jobId))
                    checkSessionAttributes(taskSes, sibling.getJobId().toString(), m);
            }

            try {
                taskSes.waitForAttribute("done", true, 0);
            }
            catch (InterruptedException e) {
                throw new IgniteException("Got interrupted while waiting for 'done' attribute.", e);
            }

            return null;
        }
    }
}