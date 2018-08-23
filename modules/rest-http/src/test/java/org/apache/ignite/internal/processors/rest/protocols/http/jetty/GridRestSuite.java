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
package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import junit.framework.TestSuite;

/**
 * Integration tests for Grid REST functionality; Jetty is under the hood.
 */
public class GridRestSuite extends TestSuite {
    /**
     * @return Suite that contains all tests for REST.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Apache Ignite REST Api suite");

        suite.addTest(new TestSuite(RestSetupSimpleTest.class));

        return suite;
    }
}
