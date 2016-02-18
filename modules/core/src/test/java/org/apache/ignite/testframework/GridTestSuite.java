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

package org.apache.ignite.testframework;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.CacheAbstractNewSelfTest;
import org.apache.ignite.testframework.TestsConfiguration.MultiNodeTestsConfiguration;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Grid test suite.
 */
public class GridTestSuite extends TestSuite {
    /** */
    protected final TestsConfiguration cfg;

    /**
     * @param cls Test class.
     * @param cfg Configuration.
     */
    public GridTestSuite(Class<? extends GridAbstractTest> cls, TestsConfiguration cfg) {
        super(cls);

        this.cfg = cfg;
    }


    /**
     * @param cls Test class.
     * @param cfg Configuration.
     * @param testedNodeCnt Count of tested nodes.
     */
    public static TestSuite createMultiNodeTestSuite(Class<? extends CacheAbstractNewSelfTest> cls,
        TestsConfiguration cfg,
        int testedNodeCnt) {

        if (cls.isInstance(CacheAbstractNewSelfTest.class))
            throw new IllegalArgumentException("An instance of CacheAbstractNewSelfTest expected, but was: " + cls);

        TestSuite suite = new TestSuite();

        if (cfg.gridCount() < testedNodeCnt)
            throw new IllegalArgumentException("Failed to initialize test suite [nodeCnt=" + testedNodeCnt + ", cfgGridCnt="
                + cfg.gridCount() + "]");

        int numOfTests = 0;

        for (Method m : cls.getMethods())
            if (m.getName().startsWith("test") && Modifier.isPublic(m.getModifiers()))
                numOfTests++;

        numOfTests *= testedNodeCnt;

        for (int i = 0; i < testedNodeCnt; i++) {
            MultiNodeTestsConfiguration multiNodeCfg = new MultiNodeTestsConfiguration(i, numOfTests);

            TestsConfiguration cfg0 = new TestsConfiguration(cfg.configurationFactory(), cfg.suffix(),
                cfg.isStopNodes(), cfg.cacheStartMode(), cfg.gridCount(), multiNodeCfg);

            suite.addTest(new GridTestSuite(cls, cfg0));
        }

        return suite;
    }

    /** {@inheritDoc} */
    @Override public void runTest(Test test, TestResult res) {
        if (test instanceof GridAbstractTest)
            ((GridAbstractTest)test).setTestsConfiguration(cfg);

        super.runTest(test, res);
    }
}
