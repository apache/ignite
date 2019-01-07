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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.service.IgniteServiceConfigVariationsFullApiTest;
import org.apache.ignite.testframework.configvariations.ConfigParameter;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.Parameters;
import org.apache.ignite.testframework.configvariations.VariationsTestsConfig;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

/**
 * Full API service test suit.
 */
@RunWith(IgniteServiceConfigVariationsFullApiTestSuite.DynamicSuite.class)
public class IgniteServiceConfigVariationsFullApiTestSuite {
    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigParameter<IgniteConfiguration>[][] PARAMS = new ConfigParameter[][] {
        Parameters.booleanParameters("setPeerClassLoadingEnabled")
    };

    /** */
    private static List<Class<? extends IgniteConfigVariationsAbstractTest>> suite(List<VariationsTestsConfig> cfgs) {
        List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = new ArrayList<>();

        new ConfigVariationsTestSuiteBuilder(IgniteServiceConfigVariationsFullApiTest.class)
            .igniteParams(PARAMS)
            .gridsCount(1)
            .appendTo(classes, cfgs);

        // Tests run on server (node#0) & client(node#1).
        new ConfigVariationsTestSuiteBuilder(IgniteServiceConfigVariationsFullApiTest.class)
            .igniteParams(PARAMS)
            .gridsCount(2)
            .testedNodesCount(2)
            .withClients()
            .appendTo(classes, cfgs);

        // Tests run on servers (node#0,node#2,node#3) & client(node#1).
        new ConfigVariationsTestSuiteBuilder(IgniteServiceConfigVariationsFullApiTest.class)
            .igniteParams(PARAMS)
            .gridsCount(4)
            .testedNodesCount(2)
            .withClients()
            .appendTo(classes, cfgs);

        // Tests run on servers (node#0,node#2,node#3) & client(node#1,node#4).
        new ConfigVariationsTestSuiteBuilder(IgniteServiceConfigVariationsFullApiTest.class)
            .igniteParams(PARAMS)
            .gridsCount(5)
            .testedNodesCount(2)
            .withClients()
            .appendTo(classes, cfgs);

        return classes;
    }

    /** */
    public static class DynamicSuite extends Suite {
        /** */
        private static final List<VariationsTestsConfig> cfgs = new ArrayList<>();

        /** */
        private static final List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = suite(cfgs);

        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public DynamicSuite(Class<?> cls) throws InitializationError {
            super(cls, classes.toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            IgniteConfigVariationsAbstractTest.injectTestsConfiguration(cfgs.get(cntr.getAndIncrement()));

            super.runChild(runner, ntf);
        }
    }
}
