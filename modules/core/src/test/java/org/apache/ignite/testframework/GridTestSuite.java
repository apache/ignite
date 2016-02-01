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

import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Grid test suite.
 */
public class GridTestSuite extends TestSuite {
    /** */
    private final IgniteConfiguration cfg;

    /** */
    private final String suffix;

    /**
     * @param theClass Test class.
     * @param cfg Ignite configuration.
     * @param testClsNameSuffix Test class name suffix.
     */
    public GridTestSuite(Class<? extends GridAbstractTest> theClass, IgniteConfiguration cfg, String testClsNameSuffix) {
        super(theClass);

        this.cfg = cfg;
        suffix = testClsNameSuffix;
    }

    /** {@inheritDoc} */
    @Override public void runTest(Test test, TestResult res) {
        if (test instanceof GridAbstractTest) {
            ((GridAbstractTest)test).igniteConfiguration(cfg);
            ((GridAbstractTest)test).testClassNameSuffix(suffix);
        }

        super.runTest(test, res);
    }
}
