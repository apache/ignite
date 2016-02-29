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

package org.apache.ignite.testframework.configvariations;

import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;

/**
 * Configuration variations test suite.
 */
public class IgniteConfigVariationsTestSuite extends TestSuite {
    /** */
    protected final VariationsTestsConfig cfg;

    /**
     * @param cls Test class.
     * @param cfg Configuration.
     */
    public IgniteConfigVariationsTestSuite(Class<? extends IgniteConfigVariationsAbstractTest> cls,
        VariationsTestsConfig cfg) {
        super(cls);

        this.cfg = cfg;
    }

    /**
     * @param cls Test class.
     * @param cfg Configuration.
     * @param testedNodeCnt Count of tested nodes.
     */
    public static TestSuite createMultiNodeTestSuite(Class<? extends IgniteCacheConfigVariationsAbstractTest> cls,
        VariationsTestsConfig cfg, int testedNodeCnt, boolean withClients) {
        TestSuite suite = new TestSuite();

        if (cfg.gridCount() < testedNodeCnt)
            throw new IllegalArgumentException("Failed to initialize test suite [nodeCnt=" + testedNodeCnt
                + ", cfgGridCnt=" + cfg.gridCount() + "]");

        for (int i = 0; i < testedNodeCnt; i++) {
            boolean stopNodes = cfg.isStopNodes() && i + 1 == testedNodeCnt;
            boolean startCache = i == 0;
            boolean stopCache = i + 1 == testedNodeCnt;

            VariationsTestsConfig cfg0 = new VariationsTestsConfig(cfg.configurationFactory(), cfg.description(),
                stopNodes, startCache, stopCache, cfg.cacheStartMode(), cfg.gridCount(), i, withClients);

            suite.addTest(new IgniteConfigVariationsTestSuite(cls, cfg0));
        }

        return suite;
    }

    /** {@inheritDoc} */
    @Override public void runTest(Test test, TestResult res) {
        if (test instanceof IgniteConfigVariationsAbstractTest)
            ((IgniteConfigVariationsAbstractTest)test).setTestsConfiguration(cfg);

        super.runTest(test, res);
    }
}
