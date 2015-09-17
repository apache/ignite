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
import org.apache.ignite.spi.swapspace.file.GridFileSwapCompactionSelfTest;
import org.apache.ignite.spi.swapspace.file.GridFileSwapSpaceSpiSelfTest;
import org.apache.ignite.spi.swapspace.noop.GridNoopSwapSpaceSpiSelfTest;

/**
 *
 */
public class IgniteSpiSwapSpaceSelfTestSuite {
    /**
     * @return Checkpoint test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Public Swap Space Test Suite");

        suite.addTest(new TestSuite(GridFileSwapCompactionSelfTest.class));
        suite.addTest(new TestSuite(GridFileSwapSpaceSpiSelfTest.class));
        suite.addTest(new TestSuite(GridNoopSwapSpaceSpiSelfTest.class));

        return suite;
    }
}