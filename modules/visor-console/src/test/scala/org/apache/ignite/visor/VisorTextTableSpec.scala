/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.visor

import org.apache.ignite.visor.commands.common.VisorTextTable

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
