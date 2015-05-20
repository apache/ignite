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

package org.apache.ignite.scalar.tests.examples

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.examples.computegrid.failover.ComputeFailoverNodeStartup
import org.apache.ignite.examples.misc.client.memcache.MemcacheRestExampleNodeStartup
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.scalar.examples.computegrid._
import org.apache.ignite.scalar.examples.computegrid.cluster.ScalarComputeClusterGroupExample
import org.apache.ignite.scalar.examples.computegrid.failover.ScalarComputeFailoverExample
import org.apache.ignite.scalar.examples.computegrid.montecarlo.ScalarComputeCreditRiskExample
import org.apache.ignite.scalar.examples.datagrid._
import org.apache.ignite.scalar.examples.datagrid.hibernate.ScalarHibernateL2CacheExample
import org.apache.ignite.scalar.examples.datagrid.starschema.ScalarStarSchemaExample
import org.apache.ignite.scalar.examples.datagrid.store.dummy.ScalarCacheDummyStoreExample
import org.apache.ignite.scalar.examples.datagrid.store.hibernate.ScalarCacheHibernateStoreExample
import org.apache.ignite.scalar.examples.datagrid.store.jdbc.ScalarCacheJdbcStoreExample
import org.apache.ignite.scalar.examples.datastructures._
import org.apache.ignite.scalar.examples.events.ScalarEventsExample
import org.apache.ignite.scalar.examples.igfs.ScalarIgfsExample
import org.apache.ignite.scalar.examples.messaging.{ScalarMessagingExample, ScalarMessagingPingPongListenActorExample}
import org.apache.ignite.scalar.examples.misc.client.memcache.ScalarMemcacheRestExample
import org.apache.ignite.scalar.examples.misc.deployment.ScalarDeploymentExample
import org.apache.ignite.scalar.examples.misc.lifecycle.ScalarLifecycleExample
import org.apache.ignite.scalar.examples.misc.schedule.ScalarComputeScheduleExample
import org.apache.ignite.scalar.examples.misc.springbean.ScalarSpringBeanExample
import org.apache.ignite.scalar.examples.servicegrid.ScalarServicesExample
import org.apache.ignite.scalar.examples.streaming.{ScalarStreamTransformerExample, ScalarStreamVisitorExample}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest

import org.scalatest.junit.JUnitSuiteLike

/**
 * Scalar examples self test.
 */
class ScalarExamplesSelfTest extends GridAbstractExamplesTest with JUnitSuiteLike {
    /** */
    private def EMPTY_ARGS = Array.empty[String]

    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    private def runWithNode(cfgPath: String, f: () => Unit) {
        val ignite = IgnitionEx.start(cfgPath, "secondNode")

        try {
            f()
        }
        finally {
            ignite.close()
        }
    }

    private def runWithNode(cfg: IgniteConfiguration, f: () => Unit) {
        cfg.setGridName("secondNode")

        scalar(cfg) {
            f()
        }
    }
    /** Compute grid examples */
    
    /** */
    def testScalarComputeClusterGroupExample() {
        runWithNode(CONFIG, () => ScalarComputeClusterGroupExample.main(EMPTY_ARGS))
    }

    /** */
    def testScalarComputeAsyncExample() {
        ScalarComputeAsyncExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeBroadcastExample() {
        ScalarComputeBroadcastExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeCallableExample() {
        ScalarComputeCallableExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeClosureExample() {
        ScalarComputeClosureExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeContinuousMapperExample() {
        ScalarComputeContinuousMapperExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeFibonacciContinuationExample() {
        ScalarComputeFibonacciContinuationExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeReduceExample() {
        ScalarComputeReduceExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeRunnableExample() {
        ScalarComputeRunnableExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeTaskMapExample() {
        ScalarComputeTaskMapExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeTaskSplitExample() {
        ScalarComputeTaskSplitExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeCreditRiskExample() {
        ScalarComputeCreditRiskExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeFailoverExample() {
        runWithNode(ComputeFailoverNodeStartup.configuration(), () => ScalarComputeFailoverExample.main(EMPTY_ARGS))
    }

    /** Data grid example */

    /** */
    def testScalarHibernateL2CacheExample() {
        ScalarHibernateL2CacheExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarStarSchemaExample() {
        ScalarStarSchemaExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheDummyStoreExample() {
        ScalarCacheDummyStoreExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheHibernateStoreExample() {
        ScalarCacheHibernateStoreExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheJdbcStoreExample() {
        ScalarCacheJdbcStoreExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheAffinityExample() {
        ScalarCacheAffinityExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheApiExample() {
        ScalarCacheApiExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheAsyncApiExample() {
        ScalarCacheAsyncApiExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheContinuousQueryExample() {
        ScalarCacheContinuousQueryExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheDataStreamerExample() {
        ScalarCacheDataStreamerExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheEventsExample() {
        ScalarCacheEventsExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCachePutGetExample() {
        ScalarCachePutGetExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheQueryExample() {
        ScalarCacheQueryExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheTransactionExample() {
        ScalarCacheTransactionExample.main(EMPTY_ARGS)
    }

    /** Data structures examples */

    /** */
    def testScalarExecutorServiceExample() {
        ScalarExecutorServiceExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarIgniteAtomicLongExample() {
        ScalarIgniteAtomicLongExampleStartup.main(EMPTY_ARGS)
    }

    /** */
    def testScalarIgniteAtomicSequenceExample() {
        ScalarIgniteAtomicSequenceExampleStartup.main(EMPTY_ARGS)
    }

    /** */
    def testScalarIgniteAtomicStampedExample() {
        ScalarIgniteAtomicStampedExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarIgniteCountDownLatchExample() {
        ScalarIgniteCountDownLatchExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarIgniteQueueExample() {
        ScalarIgniteQueueExampleStartup.main(EMPTY_ARGS)
    }

    /** */
    def testScalarIgniteSetExample() {
        ScalarIgniteSetExample.main(EMPTY_ARGS)
    }

    /** Event examples */

    /** */
    def testScalarEventsExample() {
        ScalarEventsExample.main(EMPTY_ARGS)
    }

    /** IGFS examples */

    /** */
    def testScalarIgfsExample() {
        runWithNode("examples/config/filesystem/example-igfs.xml", () => ScalarIgfsExample.main(EMPTY_ARGS))
    }

    /** Messaging examples */

    /** */
    def testScalarMessagingExample() {
        runWithNode(CONFIG, () => ScalarMessagingExample.main(EMPTY_ARGS))
    }

    /** */
    def testScalarMessagingPingPongListenActorExample() {
        runWithNode("modules/scalar/src/test/resources/spring-ping-pong-partner.xml",
            () => ScalarMessagingPingPongListenActorExample.main(EMPTY_ARGS))
    }

    /** Misc examples */

    /** */
    def testScalarDeploymentExample() {
        ScalarDeploymentExample.main(EMPTY_ARGS)
    }

    /** */
    def testMemcacheRestExampleNode() {
        runWithNode(MemcacheRestExampleNodeStartup.configuration(), () => ScalarMemcacheRestExample.main(EMPTY_ARGS))
    }

    /** */
    def testScalarLifecycleExample() {
        ScalarLifecycleExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarComputeScheduleExample() {
        ScalarComputeScheduleExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarSpringBeanExample() {
        ScalarSpringBeanExample.main(EMPTY_ARGS)
    }

    /** Service grid examples */

    /** */
    def testScalarServicesExample() {
        ScalarServicesExample.main(EMPTY_ARGS)
    }

    /** Streaming examples */

    /** */
    def testScalarStreamTransformerExample() {
        ScalarStreamTransformerExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarStreamVisitorExample() {
        ScalarStreamVisitorExample.main(EMPTY_ARGS)
    }
}
