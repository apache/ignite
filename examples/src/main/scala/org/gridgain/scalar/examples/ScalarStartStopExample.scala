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

package org.gridgain.scalar.examples

import org.apache.ignite.Ignite
import org.gridgain.scalar.scalar
import scalar._

/**
 * Demonstrates various starting and stopping ways of grid using Scalar.
 * <p>
 * Grid nodes in this example start with the default configuration file `'GRIDGAIN_HOME/config/default-config.xml'`.
 * <p>
 * Remote nodes should also be started with the default one: `'ggstart.{sh|bat}'`.
 */
object ScalarStartStopExample {
    /**
     * Example entry point. No arguments required.
     */
    def main(args: Array[String]) {
        way1()
        way2()
        way3()
    }

    /**
     * One way to start GridGain.
     */
    def way1() {
        scalar {
            println("Hurrah - I'm in the grid!")
            println("Local node ID is: " + grid$.cluster().localNode.id)
        }
    }

    /**
     * One way to start GridGain.
     */
    def way2() {
        scalar.start()

        try {
            println("Hurrah - I'm in the grid!")
        }
        finally {
            scalar.stop()
        }
    }

    /**
     * One way to start GridGain.
     */
    def way3() {
        scalar { g: Ignite =>
            println("Hurrah - local node ID is: " + g.cluster().localNode.id)
        }
    }
}
