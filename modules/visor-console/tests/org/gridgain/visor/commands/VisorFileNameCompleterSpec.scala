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

import java.util
import org.scalatest.matchers._
import org.scalatest._
import java.io.File

/**
 * Test for visor's file name completer.
 *
 * @author @java.author
 * @version @java.version
 */
class VisorFileNameCompleterSpec extends FlatSpec with ShouldMatchers {
    behavior of "A visor file name completer"

    it should "properly parse empty path" in {
        val c = new VisorFileNameCompleter()

        val res = new util.ArrayList[CharSequence]()

        c.complete("", 0, res)

        assertResult(new File("").getAbsoluteFile.listFiles().length)(res.size)

        res.clear()

        c.complete(null, 0, res)

        assertResult(new File("").getAbsoluteFile.listFiles().length)(res.size)

        res.clear()

        c.complete("    ", 2, res)

        assertResult(new File("").getAbsoluteFile.listFiles().length)(res.size)

        res.clear()

        c.complete("help ", 5, res)

        assertResult(new File("").getAbsoluteFile.listFiles().length)(res.size)
    }
}
