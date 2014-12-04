/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.alert

import java.util.regex.Pattern

import org.apache.ignite.configuration.IgniteConfiguration
import org.gridgain.grid.spi.discovery.GridDiscoverySpi
import org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder
import org.gridgain.grid.{GridGain => G}
import org.gridgain.visor._
import org.gridgain.visor.commands.alert.VisorAlertCommand._

/**
 * Unit test for alert commands.
 */
class VisorAlertCommandSpec extends VisorRuntimeBaseSpec(1) {
    /** */
    val ipFinder = new GridTcpDiscoveryVmIpFinder(true)

    /**  */
    val out = new java.io.ByteArrayOutputStream

    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Grid name.
     * @return Grid configuration.
     */
    override def config(name: String): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setGridName(name)
        cfg.setLifeCycleEmailNotification(false)
        cfg.setLocalHost("127.0.0.1")

        val discoSpi: GridTcpDiscoverySpi = new GridTcpDiscoverySpi()

        discoSpi.setIpFinder(ipFinder)

        cfg.setDiscoverySpi(discoSpi.asInstanceOf[GridDiscoverySpi])

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

    behavior of "An 'alert' visor command"

    it should "print not connected error message" in {
        visor.close()

        checkOut(visor.alert("-r -t=5 -cc=gte4"), "Visor is disconnected.")

        checkOut(visor.alert(), "No alerts are registered.")
    }

    it should "register new alert" in {
        try {
            checkOut(visor.alert(), "No alerts are registered.")

            matchOut(visor.alert("-r -t=5 -cc=gte4"), "Alert.+registered.")

            checkOut(visor.alert(), "No alerts are registered.", false)
        }
        finally {
            visor.alert("-u -a")
        }
    }

    it should "print error messages on incorrect alerts" in {
        try {
            matchOut(visor.alert("-r -t=5"), "Alert.+registered.")

            checkOut(visor.alert("-r -UNKNOWN_KEY=lt20"), "Invalid argument")

            checkOut(visor.alert("-r -cc=UNKNOWN_OPERATION20"), "Invalid expression")
        }
        finally {
            visor.alert("-u -a")
        }
    }

    it should "write alert to log" in {
        try {
            matchOut(visor.alert("-r -nc=gte1"), "Alert.+registered.")

            G.start(config("node-2"))

            G.stop("node-2", false)

            checkOut(visor.alert(), "No alerts are registered.", false)
        }
        finally {
            visor.alert("-u -a")
        }
    }
}
