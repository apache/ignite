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

package org.apache.ignite.internal.processors.cache.persistence.standbycluster.join;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;

/**
 * Printout baseline tests
 */
public class PrintoutBaselineTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 10;

    /** Loggers. */
    private MockLogger[] loggers = new MockLogger[NODES_CNT];

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(20 * 1024 * 1024).setPersistenceEnabled(true)
            )
        );

        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        MockLogger log = loggers[idx];

        if (log == null)
            loggers[idx] = log = new MockLogger();

        cfg.setGridLogger(log);

        cfg.setAutoActivationEnabled(true);

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /**
     *
     */
    public void testBaselinePrintout() throws Exception {
        cleanPersistenceDir();

        for(int i = 0; i < NODES_CNT; i++)
            loggers[i] = new MockLogger();

        startGrid(0);
        startGrid(1);
        startGrid(2);
        startGrid(3);
        startGrid(4);

        RegexpMessageReducer clusterStateMsgReducer = new RegexpMessageReducer("clusterState=([A-Z]+)");

        RegexpMessageReducer baselineSizeMsgReducer = new RegexpMessageReducer(
            "\\^-- Baseline \\[id=\\d+, size=(\\d+), online=\\d+, offline=\\d+\\]");

        RegexpMessageReducer baselineOnlineCntMsgReducer = new RegexpMessageReducer(
            "\\^-- Baseline \\[id=\\d+, size=\\d+, online=(\\d+), offline=\\d+\\]");

        RegexpMessageReducer autoActivationNodesCntMsgReducer = new RegexpMessageReducer(
            "\\^-- (\\d+) nodes left for auto-activation");

        RegexpMessageReducer autoActivationNodesIdsMsgReducer = new RegexpMessageReducer(
            "nodes left for auto-activation \\[(.+)\\]");

        loggers[0].addReducer(clusterStateMsgReducer);
        loggers[0].addReducer(baselineSizeMsgReducer);
        loggers[0].addReducer(baselineOnlineCntMsgReducer);
        loggers[0].addReducer(autoActivationNodesCntMsgReducer);
        loggers[0].addReducer(autoActivationNodesIdsMsgReducer);

        startGrid(5);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("INACTIVE", clusterStateMsgReducer.reduce());
        assertNull(baselineSizeMsgReducer.reduce());
        assertNull(baselineOnlineCntMsgReducer.reduce());
        assertNull(autoActivationNodesCntMsgReducer.reduce());
        assertNull(autoActivationNodesIdsMsgReducer.reduce());

        startGrid(6);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("INACTIVE", clusterStateMsgReducer.reduce());
        assertNull(baselineSizeMsgReducer.reduce());
        assertNull(baselineOnlineCntMsgReducer.reduce());
        assertNull(autoActivationNodesCntMsgReducer.reduce());
        assertNull(autoActivationNodesIdsMsgReducer.reduce());

        grid(0).cluster().active(true);

        startGrid(7);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("ACTIVE", clusterStateMsgReducer.reduce());
        assertEquals("7", baselineSizeMsgReducer.reduce());
        assertEquals("7", baselineOnlineCntMsgReducer.reduce());
        assertNull(autoActivationNodesCntMsgReducer.reduce());
        assertNull(autoActivationNodesIdsMsgReducer.reduce());

        stopGrid(7);
        stopGrid(6);

        loggers[0].resetReducers();

        stopGrid(5);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("ACTIVE", clusterStateMsgReducer.reduce());
        assertEquals("7", baselineSizeMsgReducer.reduce());
        assertEquals("5", baselineOnlineCntMsgReducer.reduce());
        assertNull(autoActivationNodesCntMsgReducer.reduce());
        assertNull(autoActivationNodesIdsMsgReducer.reduce());

        grid(0).cluster().active(false);

        startGrid(5);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("INACTIVE", clusterStateMsgReducer.reduce());
        assertEquals("7", baselineSizeMsgReducer.reduce());
        assertEquals("6", baselineOnlineCntMsgReducer.reduce());
        assertEquals("1", autoActivationNodesCntMsgReducer.reduce());
        assertEquals(getTestIgniteInstanceName(6), autoActivationNodesIdsMsgReducer.reduce());

        startGrid(7);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("INACTIVE", clusterStateMsgReducer.reduce());
        assertEquals("7", baselineSizeMsgReducer.reduce());
        assertEquals("6", baselineOnlineCntMsgReducer.reduce());
        assertEquals("1", autoActivationNodesCntMsgReducer.reduce());
        assertEquals(getTestIgniteInstanceName(6), autoActivationNodesIdsMsgReducer.reduce());

        startGrid(6);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("INACTIVE", clusterStateMsgReducer.reduce());
        assertEquals("7", baselineSizeMsgReducer.reduce());
        assertEquals("7", baselineOnlineCntMsgReducer.reduce());
        assertEquals("0", autoActivationNodesCntMsgReducer.reduce());
        assertNull(autoActivationNodesIdsMsgReducer.reduce());

        stopGrid(7);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("ACTIVE", clusterStateMsgReducer.reduce());
        assertEquals("7", baselineSizeMsgReducer.reduce());
        assertEquals("7", baselineOnlineCntMsgReducer.reduce());
        assertNull("0", autoActivationNodesCntMsgReducer.reduce());
        assertNull(autoActivationNodesIdsMsgReducer.reduce());

        stopAllGrids();

        loggers[0].resetReducers();

        startGrid(0);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("INACTIVE", clusterStateMsgReducer.reduce());
        assertEquals("7", baselineSizeMsgReducer.reduce());
        assertEquals("1", baselineOnlineCntMsgReducer.reduce());
        assertEquals("6", autoActivationNodesCntMsgReducer.reduce());
        assertNull(autoActivationNodesIdsMsgReducer.reduce());

        startGrid(1);

        assertTrue(clusterStateMsgReducer.waitForResult(5_000L));
        assertEquals("INACTIVE", clusterStateMsgReducer.reduce());
        assertEquals("7", baselineSizeMsgReducer.reduce());
        assertEquals("2", baselineOnlineCntMsgReducer.reduce());
        assertEquals("5", autoActivationNodesCntMsgReducer.reduce());
        assertNotNull(autoActivationNodesIdsMsgReducer.reduce());

        loggers[0].clearReducers();
    }

    /**
     *
     */
    private static class RegexpMessageReducer extends MessageReducer<String> {
        /**
         * @param regexp Regexp.
         */
        public RegexpMessageReducer(final String regexp) {
            super(
                new IgniteClosure<String, String>() {
                    @Override public String apply(String s) {
                        Pattern ptrn = Pattern.compile(regexp);
                        Matcher matcher = ptrn.matcher(s);

                        if (matcher.find())
                            return matcher.group(1);
                        else
                            return null;
                    }
                }
            );
        }
    }

    /**
     *
     */
    private static class MessageReducer<T> {
        /** Result. */
        private T res;

        /** Closure. */
        private final IgniteClosure<String, T> clo;

        /**
         * @param clo Closure.
         */
        public MessageReducer(IgniteClosure<String, T> clo) {
            this.clo = clo;
        }

        /**
         * @param msg Message.
         */
        public void collect(String msg) {
            T res = clo.apply(msg);

            if (res != null && this.res == null)
                this.res = res;
        }

        /**
         *
         */
        public T reduce() {
            T res = this.res;

            this.res = null;

            return res;
        }

        /**
         * @param timeout Timeout.
         */
        public boolean waitForResult(long timeout) throws IgniteInterruptedCheckedException {
            return GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return res != null;
                }
            }, timeout);
        }
    }

    /**
     * Mock logger.
     */
    private static class MockLogger extends GridTestLog4jLogger {
        /** Reducers. */
        List<MessageReducer<?>> msgReducers = new ArrayList<>();

        /**
         * @param msgReducer MessageReducer.
         */
        public void addReducer(MessageReducer<?> msgReducer) {
            msgReducers.add(msgReducer);
        }

        /**
         * @param msgReducer MessageReducer.
         */
        public void removeReducer(MessageReducer<?> msgReducer) {
            msgReducers.remove(msgReducer);
        }

        /**
         *
         */
        public void clearReducers() {
            msgReducers.clear();
        }

        /**
         *
         */
        public void resetReducers() {
            for (MessageReducer<?> reducer : msgReducers)
                reducer.reduce();
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            super.info(msg);

            for (MessageReducer<?> messageReducer : msgReducers)
                messageReducer.collect(msg);
        }

        /** {@inheritDoc} */
        @Override public GridTestLog4jLogger getLogger(Object ctgr) {
            return this;
        }
    }
}
