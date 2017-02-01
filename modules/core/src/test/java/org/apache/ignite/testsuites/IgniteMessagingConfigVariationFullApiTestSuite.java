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
import org.apache.ignite.internal.processors.messaging.IgniteMessagingConfigVariationFullApiTest;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.configvariations.ConfigParameter;
import org.apache.ignite.testframework.configvariations.ConfigVariations;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.Parameters;

/**
 * Test sute for Messaging process.
 */
public class IgniteMessagingConfigVariationFullApiTestSuite extends TestSuite {
    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigParameter<IgniteConfiguration>[][] GRID_PARAMETER_VARIATION = new ConfigParameter[][] {
        Parameters.objectParameters("setMarshaller",
            Parameters.factory(JdkMarshaller.class),
            Parameters.factory(BinaryMarshaller.class),
            ConfigVariations.optimizedMarshallerFactory()
        ),
        Parameters.booleanParameters("setPeerClassLoadingEnabled")
    };

    /**
     * @return Messaging test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Compute New Full API Test Suite");

        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "Single server",
            IgniteMessagingConfigVariationFullApiTest.class)
            .gridsCount(1)
            .igniteParams(GRID_PARAMETER_VARIATION)
            .build());

        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "Multiple servers and client",
            IgniteMessagingConfigVariationFullApiTest.class)
            .testedNodesCount(2)
            .gridsCount(6)
            .withClients()
            .igniteParams(GRID_PARAMETER_VARIATION)
            .build());

        return suite;
    }
}
