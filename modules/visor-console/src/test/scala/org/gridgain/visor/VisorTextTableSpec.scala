/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor

import org.gridgain.visor.commands.VisorTextTable
import org.scalatest._

import scala.collection._

/**
 * Test for visor text table.
 */
class VisorTextTableSpec extends FlatSpec with ShouldMatchers {
    "A table with header" should "render" in {
        val t = new VisorTextTable()

        t.margin(5, 5, 5, 5)

        t.maxCellWidth = 10

        t.headerStyle("leftPad: 10, rightPad: 5")
        t #= ("Header 1", mutable.Seq("Header 2.1", "Header 2.2"), "Header 3")
        t += ("Row 1", mutable.ListBuffer("Row 2"), immutable.List("Row 3.1", "Row 3.2"))
        t += ("1234567890zxcvbnmasdASDFGHJKLQ", mutable.ListBuffer("Row 2"), immutable.List("Row 3.1", "Row 3.2"))
        t += (immutable.Seq("Row 31.1", "Row 31.2"), "Row 11", "Row 21")

        t.render()
    }
}
