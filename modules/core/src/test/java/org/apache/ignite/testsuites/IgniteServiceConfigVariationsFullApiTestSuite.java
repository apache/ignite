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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.service.IgniteServiceConfigVariationsFullApiTest;
import org.apache.ignite.testframework.configvariations.ConfigParameter;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.Parameters;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * Full API service test suite.
 */
//@ExtendWith(DynamicSuite.class)
public class IgniteServiceConfigVariationsFullApiTestSuite {
    /** */
    @BeforeAll
    public static void init() {
        //JUnitTeamcityReporter.suite = IgniteServiceConfigVariationsFullApiTestSuite.class.getName();
    }

    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigParameter<IgniteConfiguration>[][] PARAMS = new ConfigParameter[][] {
        Parameters.booleanParameters("setPeerClassLoadingEnabled")
    };

    /** */
    @Test
    public void suite() {
        List<Class<?>> variants = Stream.of(
                        new ConfigVariationsTestSuiteBuilder(IgniteServiceConfigVariationsFullApiTest.class)
                                .igniteParams(PARAMS)
                                .gridsCount(1)
                                .classes(),

                        // Tests run on server (node#0) & client(node#1).
                        new ConfigVariationsTestSuiteBuilder(IgniteServiceConfigVariationsFullApiTest.class)
                                .igniteParams(PARAMS)
                                .gridsCount(2)
                                .testedNodesCount(2)
                                .withClients()
                                .classes(),

                        // Tests run on servers (node#0,node#2,node#3) & client(node#1).
                        new ConfigVariationsTestSuiteBuilder(IgniteServiceConfigVariationsFullApiTest.class)
                                .igniteParams(PARAMS)
                                .gridsCount(4)
                                .testedNodesCount(2)
                                .withClients()
                                .classes(),

                        // Tests run on servers (node#0,node#2,node#3) & client(node#1,node#4).
                        new ConfigVariationsTestSuiteBuilder(IgniteServiceConfigVariationsFullApiTest.class)
                                .igniteParams(PARAMS)
                                .gridsCount(5)
                                .testedNodesCount(2)
                                .withClients()
                                .classes())

                .flatMap(Collection::stream).collect(Collectors.toList());

        for (Class<?> cls : variants) {
            final LauncherDiscoveryRequest request =
                    LauncherDiscoveryRequestBuilder.request()
                            .selectors(selectClass(cls))
                            .build();

            final Launcher launcher = LauncherFactory.create();
            final SummaryGeneratingListener listener = new SummaryGeneratingListener();

            launcher.registerTestExecutionListeners(listener);
            launcher.execute(request);

            TestExecutionSummary summary = listener.getSummary();
            //long testFoundCount = summary.getTestsFoundCount();
            List<TestExecutionSummary.Failure> failures = summary.getFailures();
            System.out.println("getTestsSucceededCount() - " + summary.getTestsSucceededCount());
            failures.forEach(failure -> System.out.println("failure - " + failure.getException()));
        }
    }
}
