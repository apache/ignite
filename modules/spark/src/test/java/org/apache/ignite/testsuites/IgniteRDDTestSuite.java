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

import junit.framework.TestSuite;
import org.apache.ignite.spark.JavaEmbeddedIgniteRDDSelfTest;
import org.apache.ignite.spark.JavaStandaloneIgniteRDDSelfTest;

/**
 * Test suit for Ignite RDD
 */
public class IgniteRDDTestSuite extends TestSuite {
    /**
     * @return Java Ignite RDD test suit.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Java Ignite RDD tests (standalone and embedded modes");

        suite.addTest(new TestSuite(JavaEmbeddedIgniteRDDSelfTest.class));
        suite.addTest(new TestSuite(JavaStandaloneIgniteRDDSelfTest.class));

        return suite;
    }
}