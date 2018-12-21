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
package org.apache.ignite.testsuites;

import java.util.HashSet;
import junit.framework.TestSuite;
import org.apache.ignite.IgniteSystemProperties;

/**
 * Mvcc version of {@link IgnitePdsTestSuite3}.
 */
public class IgnitePdsMvccTestSuite3 extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        TestSuite suite = new TestSuite("Ignite Persistent Store Mvcc Test Suite 3");

        HashSet<Class> ignoredTests = new HashSet<>();

        // No ignored tests yet.

        suite.addTest(IgnitePdsTestSuite3.suite(ignoredTests));

        return suite;
    }
}