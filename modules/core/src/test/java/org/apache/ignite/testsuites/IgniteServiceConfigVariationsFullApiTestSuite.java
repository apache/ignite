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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.service.IgniteServiceConfigVariationsFullApiTest;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.configvariations.ConfigParameter;
import org.apache.ignite.testframework.configvariations.ConfigVariations;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.Parameters;

/**
 * Full API service test suit.
 */
public class IgniteServiceConfigVariationsFullApiTestSuite extends TestSuite {
    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigParameter<IgniteConfiguration>[][] PARAMS = new ConfigParameter[][] {
        Parameters.objectParameters("setMarshaller",
            Parameters.factory(JdkMarshaller.class),
            Parameters.factory(BinaryMarshaller.class),
            ConfigVariations.binaryMarshallerFactory()
        ),

        Parameters.booleanParameters("setPeerClassLoadingEnabled")
    };

    /**
     * @return Compute API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Service Deployment New Full API Test Suite");

        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "Single server",
            IgniteServiceConfigVariationsFullApiTest.class)
            .igniteParams(PARAMS)
            .gridsCount(1)
            .build());

        // Tests run on server (node#0) & client(node#1).
        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "1 server, 1 client",
            IgniteServiceConfigVariationsFullApiTest.class)
            .igniteParams(PARAMS)
            .gridsCount(2)
            .testedNodesCount(2)
            .withClients()
            .build());

        // Tests run on servers (node#0,node#2,node#3) & client(node#1).
        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "3 servers, 1 client",
            IgniteServiceConfigVariationsFullApiTest.class)
            .igniteParams(PARAMS)
            .gridsCount(4)
            .testedNodesCount(2)
            .withClients()
            .build());

        // Tests run on servers (node#0,node#2,node#3) & client(node#1,node#4).
        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "3 servers, 2 clients",
            IgniteServiceConfigVariationsFullApiTest.class)
            .igniteParams(PARAMS)
            .gridsCount(5)
            .testedNodesCount(2)
            .withClients()
            .build());

        return suite;
    }
}