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

package org.apache.ignite.visor.commands.alert

import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spi.discovery.DiscoverySpi
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder

import java.util.regex.Pattern

import org.apache.ignite.visor.{VisorRuntimeBaseSpec, visor}
import org.apache.ignite.visor.commands.alert.VisorAlertCommand._

/**
 * Unit test for alert commands.
 */
class VisorAlertCommandSpec extends VisorRuntimeBaseSpec(1) {
    /** */
    val ipFinder = new TcpDiscoveryVmIpFinder(true)

    /**  */
    val out = new java.io.ByteArrayOutputStream

    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Ignite instance name.
     * @return Grid configuration.
     */
    override def config(name: String): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setIgniteInstanceName(name)
        cfg.setLocalHost("127.0.0.1")

        val discoSpi: TcpDiscoverySpi = new TcpDiscoverySpi()

        discoSpi.setIpFinder(ipFinder)

        cfg.setDiscoverySpi(discoSpi.asInstanceOf[DiscoverySpi])

        cfg
    }

    override def afterAll() {
        super.afterAll()

        out.close()
    }

    /**
     * Redirect stdout and compare output with specified text.
     *
     * @param block Function to execute.
     * @param text Text to compare with.
     * @param exp If `true` then stdout must contain `text` otherwise must not.
     */
    private[this] def checkOut(block: => Unit, text: String, exp: Boolean = true) {
        try {
            Console.withOut(out)(block)

            assertResult(exp)(out.toString.contains(text))
        }
        finally {
            out.reset()
        }
    }

    /**
     * Redirect stdout and compare output with specified regexp.
     *
     * @param block Function to execute.
     * @param regex Regexp to match with.
     */
    private[this] def matchOut(block: => Unit, regex: String) {
        try {
            Console.withOut(out)(block)

            assertResult(true)(Pattern.compile(regex, Pattern.MULTILINE).matcher(out.toString).find())
        }
        finally {
            out.reset()
        }
    }

    describe("An 'alert' visor command") {
        it("should print not connected error message") {
            visor.close()

            checkOut(visor.alert("-r -t=5 -cc=gte4"), "Visor is disconnected.")

            checkOut(visor.alert(), "No alerts are registered.")
        }

        it("should register new alert") {
            try {
                checkOut(visor.alert(), "No alerts are registered.")

                matchOut(visor.alert("-r -t=5 -cc=gte4"), "Alert.+registered.")

                checkOut(visor.alert(), "No alerts are registered.", false)
            }
            finally {
                visor.alert("-u -a")
            }
        }

        it("should print error messages on incorrect alerts") {
            try {
                checkOut(visor.alert("-r -t=5"), "No predicates have been provided in args")

                checkOut(visor.alert("-r -UNKNOWN_KEY=lt20"), "Invalid argument")

                checkOut(visor.alert("-r -cc=UNKNOWN_OPERATION20"), "Invalid expression")
            }
            finally {
                visor.alert("-u -a")
            }
        }

        it("should write alert to log") {
            try {
                matchOut(visor.alert("-r -nc=gte1"), "Alert.+registered.")

                Ignition.start(config("node-2"))

                Ignition.stop("node-2", false)

                checkOut(visor.alert(), "No alerts are registered.", false)
            }
            finally {
                visor.alert("-u -a")
            }
        }
    }
}
