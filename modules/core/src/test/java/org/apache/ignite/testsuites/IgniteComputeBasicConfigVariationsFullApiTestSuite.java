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
import org.apache.ignite.internal.processors.compute.IgniteComputeConfigVariationsFullApiTest;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.configvariations.ConfigParameter;
import org.apache.ignite.testframework.configvariations.ConfigVariations;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.Parameters;

/**
 * Full API compute test.
 */
public class IgniteComputeBasicConfigVariationsFullApiTestSuite extends TestSuite {
    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigParameter<IgniteConfiguration>[][] BASIC_COMPUTE_SET = new ConfigParameter[][] {
        Parameters.objectParameters("setMarshaller",
            Parameters.factory(JdkMarshaller.class),
            Parameters.factory(BinaryMarshaller.class),
            ConfigVariations.binaryMarshallerFactory()
        ),
        Parameters.booleanParameters("setPeerClassLoadingEnabled"),
        Parameters.booleanParameters("setMarshalLocalJobs"),
    };

    /**
     * @return Compute API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Compute New Full API Test Suite");

        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "Single server",
            IgniteComputeConfigVariationsFullApiTest.class)
            .igniteParams(BASIC_COMPUTE_SET)
            .gridsCount(1)
            .build());

        // Tests run on server (node#0) & client(node#1).
        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "3 servers, 1 client",
            IgniteComputeConfigVariationsFullApiTest.class)
            .igniteParams(BASIC_COMPUTE_SET)
            .gridsCount(4)
            .testedNodesCount(2)
            .withClients()
            .build());

        return suite;
    }
}
