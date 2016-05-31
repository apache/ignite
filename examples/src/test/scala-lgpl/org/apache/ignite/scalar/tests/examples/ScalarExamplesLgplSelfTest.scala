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

import org.apache.ignite.examples.util.DbH2ServerStartup
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.scalar.examples.datagrid.hibernate.ScalarHibernateL2CacheExample
import org.apache.ignite.scalar.examples.datagrid.store.hibernate.ScalarCacheHibernateStoreExampleStartup
import org.apache.ignite.scalar.examples.misc.schedule.ScalarComputeScheduleExample
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest
import org.scalatest.junit.JUnitSuiteLike

/**
 * Scalar examples self test.
 */
class ScalarExamplesLgplSelfTest extends GridAbstractExamplesTest with JUnitSuiteLike {
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

    // Compute examples

    /** */
    def testScalarComputeScheduleExample() {
        ScalarComputeScheduleExample.main(EMPTY_ARGS)
    }

    // Datagrid example

    /** */
    def testScalarHibernateL2CacheExample() {
        ScalarHibernateL2CacheExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheHibernateStoreExample() {
        runWithNode(CONFIG, () => ScalarCacheHibernateStoreExampleStartup.main(EMPTY_ARGS))
    }
}
