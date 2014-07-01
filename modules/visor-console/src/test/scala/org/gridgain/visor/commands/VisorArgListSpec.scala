/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands

import org.gridgain.visor.visor._
import org.scalatest._

/**
 * Test for visor's argument list parsing.
 */
class VisorArgListSpec extends FlatSpec with ShouldMatchers {
    behavior of "A visor argument list"

    it should "properly parse 'null' arguments" in {
        val v = parseArgs(null)

        assert(v.isEmpty)
    }

    it should "properly parse non-empty arguments" in {
        val v = parseArgs("-a=b c d -minus -d=")

        assert(v.size == 5)

        assert(v(0)._1 == "a")
        assert(v(0)._2 == "b")

        assert(v(1)._1 == null)
        assert(v(1)._2 == "c")

        assert(v(2)._1 == null)
        assert(v(2)._2 == "d")

        assert(v(3)._1 == "minus")
        assert(v(3)._2 == null)

        assert(v(4)._1 == "d")
        assert(v(4)._2 == "")
    }

    it should "properly parse quoted arguments" in {
        val v = parseArgs("-a='b 'c' d' -minus -d=")

        assert(v.size == 3)

        assert(v(0)._1 == "a")
        assert(v(0)._2 == "b 'c' d")

        assert(v(1)._1 == "minus")
        assert(v(1)._2 == null)

        assert(v(2)._1 == "d")
        assert(v(2)._2 == "")
    }
}
