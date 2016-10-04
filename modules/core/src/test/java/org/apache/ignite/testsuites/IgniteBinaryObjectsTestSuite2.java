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
import org.apache.ignite.IgniteSystemProperties;

/**
 * Test for binary objects stored in cache using {@link IgniteSystemProperties#IGNITE_BINARY_COMPACT_ZEROES}
 *
 * @deprecated IGNITE_BINARY_COMPACT_ZEROES should be default mode in Apache Ignite 2.0, so this test will be redundant.
 */
@Deprecated
public class IgniteBinaryObjectsTestSuite2 extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_BINARY_COMPACT_ZEROES, "true");

        return IgniteBinaryObjectsTestSuite.suite();
    }
}
