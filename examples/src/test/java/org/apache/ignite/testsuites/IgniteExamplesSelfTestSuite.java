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
import org.apache.ignite.examples.BasicExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.BasicExamplesSelfTest;
import org.apache.ignite.examples.CacheClientPortableExampleTest;
import org.apache.ignite.examples.CacheExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.CacheExamplesSelfTest;
import org.apache.ignite.examples.CheckpointExamplesSelfTest;
import org.apache.ignite.examples.ClusterGroupExampleSelfTest;
import org.apache.ignite.examples.ComputeClientPortableExampleTest;
import org.apache.ignite.examples.ContinuationExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.ContinuationExamplesSelfTest;
import org.apache.ignite.examples.ContinuousMapperExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.ContinuousMapperExamplesSelfTest;
import org.apache.ignite.examples.DeploymentExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.DeploymentExamplesSelfTest;
import org.apache.ignite.examples.EventsExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.EventsExamplesSelfTest;
import org.apache.ignite.examples.HibernateL2CacheExampleMultiNodeSelfTest;
import org.apache.ignite.examples.HibernateL2CacheExampleSelfTest;
import org.apache.ignite.examples.IgfsExamplesSelfTest;
import org.apache.ignite.examples.LifecycleExamplesSelfTest;
import org.apache.ignite.examples.MemcacheRestExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.MemcacheRestExamplesSelfTest;
import org.apache.ignite.examples.MessagingExamplesSelfTest;
import org.apache.ignite.examples.MonteCarloExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.MonteCarloExamplesSelfTest;
import org.apache.ignite.examples.SpringBeanExamplesSelfTest;
import org.apache.ignite.examples.TaskExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.TaskExamplesSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Examples test suite.
 * <p>
 * Contains only Spring ignite examples tests.
 */
public class IgniteExamplesSelfTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteExamplesSelfTestSuite.class));

        TestSuite suite = new TestSuite("Ignite Examples Test Suite");

        suite.addTest(new TestSuite(CacheExamplesSelfTest.class));
        suite.addTest(new TestSuite(BasicExamplesSelfTest.class));
        suite.addTest(new TestSuite(ContinuationExamplesSelfTest.class));
        suite.addTest(new TestSuite(ContinuousMapperExamplesSelfTest.class));
        suite.addTest(new TestSuite(DeploymentExamplesSelfTest.class));
        suite.addTest(new TestSuite(EventsExamplesSelfTest.class));
        suite.addTest(new TestSuite(LifecycleExamplesSelfTest.class));
        suite.addTest(new TestSuite(MessagingExamplesSelfTest.class));
        suite.addTest(new TestSuite(MemcacheRestExamplesSelfTest.class));
        suite.addTest(new TestSuite(MonteCarloExamplesSelfTest.class));
        suite.addTest(new TestSuite(TaskExamplesSelfTest.class));
        suite.addTest(new TestSuite(SpringBeanExamplesSelfTest.class));
        suite.addTest(new TestSuite(IgfsExamplesSelfTest.class));
        suite.addTest(new TestSuite(CheckpointExamplesSelfTest.class));
        suite.addTest(new TestSuite(HibernateL2CacheExampleSelfTest.class));
        suite.addTest(new TestSuite(ClusterGroupExampleSelfTest.class));

        // Multi-node.
        suite.addTest(new TestSuite(CacheExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(BasicExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(ContinuationExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(ContinuousMapperExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(DeploymentExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(EventsExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(TaskExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(MemcacheRestExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(MonteCarloExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(HibernateL2CacheExampleMultiNodeSelfTest.class));

        // Portable.
        suite.addTest(new TestSuite(CacheClientPortableExampleTest.class));
        suite.addTest(new TestSuite(ComputeClientPortableExampleTest.class));

        return suite;
    }
}