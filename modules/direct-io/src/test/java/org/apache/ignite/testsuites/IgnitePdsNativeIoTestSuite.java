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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.persistence.file.IgniteNativeIoWithNoPersistenceTest;

/**
 * Subset of {@link IgnitePdsTestSuite} suite test, started with direct-oi jar in classpath.
 */
public class IgnitePdsNativeIoTestSuite extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite (with Direct IO)");

        IgnitePdsTestSuite.addRealPageStoreTests(suite, null);

        //long running test by design with light parameters
        suite.addTest(new JUnit4TestAdapter(IgnitePdsReplacementNativeIoTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteNativeIoWithNoPersistenceTest.class));

        return suite;
    }
}
