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

/**
 * Grid test suite.
 */
public class GridMultiNodeTestSuite extends GridTestSuite {
    /** */
    private final int testedNodeIdx;

    /** */
    private final int numOfTests;

    /**
     * @param cls Test class.
     * @param cfg Configuration.
     * @param testedNodeCnt
     */
    public static TestSuite createTestSuite(Class<? extends CacheAbstractNewSelfTest> cls, TestsConfiguration cfg,
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

        for (int i = 0; i < testedNodeCnt; i++)
            suite.addTest(new GridMultiNodeTestSuite(cls, cfg, i, numOfTests));

        return suite;
    }
    /**
     * @param cls Class.
     * @param cfg Config.
     * @param testedNodeIdx Tested node idx.
     * @param numOfTests Number of tests.
     */
    private GridMultiNodeTestSuite(Class<? extends CacheAbstractNewSelfTest> cls, TestsConfiguration cfg,
        int testedNodeIdx, int numOfTests) {
        super(cls, cfg);

        this.testedNodeIdx = testedNodeIdx;
        this.numOfTests = numOfTests;
    }


    /** {@inheritDoc} */
    @Override public void runTest(Test test, TestResult res) {
        if (test instanceof CacheAbstractNewSelfTest) {
            CacheAbstractNewSelfTest test0 = (CacheAbstractNewSelfTest)test;

            test0.setNumOfTests(numOfTests);
            test0.setTestedNodeIdx(testedNodeIdx);
        }

        super.runTest(test, res);
    }
}
