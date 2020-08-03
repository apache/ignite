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

package org.apache.ignite.visor.commands.common

import org.apache.ignite.internal.util.GridStringBuilder
import org.apache.ignite.visor.commands.common.VisorTextTable._

import scala.collection.Traversable

/**
 * ==Overview==
 * Provides `ASCII`-based table with minimal styling support.
 */
class VisorTextTable {
    /**
     * Cell style.
     */
    private sealed class Style(
        var leftPad: Int = 1, // >= 0
        var rightPad: Int = 1, // >= 0
        var align: String = "center" // center, left, right
    ) {
        assert(leftPad >= 0)
        assert(rightPad >= 0)
        assert(align != null)

        /**
         * Gets overall padding (left + right).
         */
        def padding: Int =
            leftPad + rightPad
    }

    /**
     * Cell style.
     */
    private object Style {
        /**
         *
         * @param sty Style.
         */
        def apply(sty: String): Style = {
            assert(sty != null)

            val cs = new Style

            if (!sty.isEmpty) {
                for (e <- sty.split(',')) {
                    val a = e.split(":")

                    assert(a.length == 2, "Invalid cell style: " + e.trim)

                    val a0 = a(0).trim
                    val a1 = a(1).trim

                    a0 match {
                        case "leftPad" => cs.leftPad = a1.toInt
                        case "rightPad" => cs.rightPad = a1.toInt
                        case "align" => cs.align = a1
                        case _ => assert(false, "Invalid style: " + e.trim)
                    }
                }
            }

            cs
        }
    }

    /**
     * Cell holder.
     */
    private sealed case class Cell(style: Style, lines: Seq[String]) {
        assert(style != null)
        assert(lines != null)

        /**
         * Cell's calculated width including padding.
         */
        lazy val width =
            if (height > 0)
                style.padding + lines.max(Ordering.by[String, Int](_.length)).length
            else
                style.padding

        /**
         * Gets height of the cell.
         */
        def height: Int = lines.length
    }

    /**
     * Margin holder.
     */
    private sealed case class Margin(
        top: Int = 0,
        right: Int = 0,
        bottom: Int = 0,
        left: Int = 0) {
        assert(top >= 0)
        assert(right >= 0)
        assert(bottom >= 0)
        assert(left >= 0)
    }

    /** */
    private val NL = '\n'

    /** Headers. */
    private val hdr = collection.mutable.ArrayBuffer.empty[Cell]

    /** Rows. */
    private val rows = collection.mutable.ArrayBuffer.empty[Seq[Cell]]

    /** Current row, if any. */
    private var curRow: collection.mutable.ArrayBuffer[Cell] = _

    /** Table's margin, if any. */
    private var margin: Margin = Margin()

    /** Default row cell style, if any. */
    private var rowSty: String = "align:left"

    /** Default header cell style, if any. */
    private var hdrSty: String = "align:center"

    /**
     * Flag indicating whether or not to draw inside horizontal lines
     * between individual rows.
     */
    var insideBorder = false

    /**
     * Flag indicating whether of not to automatically draw horizontal lines
     * for multi line rows.
     */
    var autoBorder = true

    /**
     * Maximum width of the cell. If any line in the cell exceeds this width
     * it will be cut in two or more lines.
     *
     * '''NOTE''': it doesn't include into account the padding. Only the actual
     * string length is counted.
     */
    var maxCellWidth = Int.MaxValue

    /**
     *
     * @param ch Char.
     * @param len Dash length.
     */
    private def dash(ch: Char, len: Int): String = {
        assert(len >= 0)

        new String().padTo(len, ch)
    }

    /**
     *
     * @param s String.
     * @param len Dash length.
     */
    private def dash(s: String, len: Int): String = {
        assert(len >= 0)

        var i = 0

        val sb = new GridStringBuilder(s.length * len)

        while (i < len) {
            sb.a(s)

            i += 1
        }

        sb.toString
    }

    /**
     *
     * @param len Length.
     */
    private def blank(len: Int): String =
        dash(' ', len)

    /**
     * Sets table's margin.
     *
     * @param top Top margin.
     * @param right Right margin.
     * @param bottom Bottom margin.
     * @param left Left margin.
     */
    def margin(top: Int = 0, right: Int = 0, bottom: Int = 0, left: Int = 0) {
        assert(top >= 0)
        assert(right >= 0)
        assert(bottom >= 0)
        assert(left >= 0)

        margin = Margin(top, right, bottom, left)
    }

    /**
     * Starts data row.
     */
    def startRow() {
        assert(curRow == null)

        curRow = collection.mutable.ArrayBuffer.empty[Cell]
    }

    /**
     * Ends data row.
     */
    def endRow() {
        assert(curRow.nonEmpty)

        rows += curRow

        curRow = null
    }

    /**
     * Adds row (one or more row cells).
     *
     * @param cells Row cells. For multi-line cells - use `Seq(...)`.
     */
    def +=(cells: Any*): VisorTextTable = {
        startRow()

        cells foreach {
            case s: scala.collection.Iterable[Any] => addRowCell(s.toSeq: _*)
            case p: Product => addRowCell(p.productIterator.toSeq: _*)
            case a => addRowCell(a)
        }

        endRow()

        this
    }

    /**
     * Adds header (one or more header cells).
     *
     * @param cells Header cells. For multi-line cells - use `Seq(...)`.
     */
    def #=(cells: Any*): VisorTextTable = {
        cells foreach {
            case s: scala.collection.Iterable[Any] => addHeaderCell(s.toSeq: _*)
            case p: Product => addHeaderCell(p.productIterator.toSeq: _*)
            case a => addHeaderCell(a)
        }

        this
    }

    /**
     * Adds single header cell.
     *
     * @param lines One or more cell lines.
     */
    def addHeaderCell(lines: Any*): VisorTextTable = {
        assert(lines != null)
        assert(lines.nonEmpty)

        // Break up long line into multiple ones - if necessary.
        val lst = lines flatten(_.toString.grouped(maxCellWidth))

        hdr += Cell(Style(hdrSty), lst)

        this
    }

    /**
     * Gets current row style.
     */
    def rowStyle =
        rowSty

    /**
     * Sets current row style.
     *
     * @param rowSty Row style to set.
     */
    def rowStyle(rowSty: String) {
        this.rowSty = rowSty
    }

    /**
     * Gets current header style.
     */
    def headerStyle =
        rowSty

    /**
     * Sets current header style.
     *
     * @param hdrSty Header style to set.
     */
    def headerStyle(hdrSty: String) {
        this.hdrSty = hdrSty
    }

    /**
     * Adds single row cell.
     *
     * @param lines One or more row cells. Multiple lines will be printed on separate lines.
     */
    def addRowCell(lines: Any*): VisorTextTable = {
        assert(lines != null)
        assert(lines.length >= 0)
        assert(curRow != null)

        // Break up long line into multiple ones - if necessary.
        val lst = lines flatten {
            case it: Traversable[_] => it.flatten(_.toString.grouped(maxCellWidth))
            case null => Seq("")
            case obj => obj.toString.grouped(maxCellWidth)
        }

        curRow += Cell(Style(rowSty), lst)

        this
    }

    /**
     *
     * @param txt Text to align.
     * @param width Width already accounts for padding.
     * @param sty Style.
     */
    private def aligned(txt: String, width: Int, sty: Style): String = {
        assert(txt != null)
        assert(width > 0)
        assert(sty != null)
        assert(txt.length <= width)

        val d = width - txt.length

        val styTxt = txt

        sty.align.trim match {
            case "center" =>
                blank(d / 2) + styTxt + blank(d / 2 + d % 2)
            case "left" =>
                blank(sty.leftPad) + styTxt + blank(d - sty.leftPad)
            case "right" =>
                blank(d - sty.rightPad) + styTxt + blank(sty.rightPad)
            case _ =>
                throw new AssertionError("Invalid align option in: " + sty)
        }
    }

    /**
     * Renders this table.
     */
    def render() {
        // Make sure table is not empty.
        if (hdr.isEmpty && rows.isEmpty)
            return

        var colsNum = -1

        val isHdr = hdr.nonEmpty

        if (isHdr)
            colsNum = hdr.size

        // Calc number of columns and make sure all rows are even.
        for (r <- rows)
            if (colsNum == -1)
                colsNum = r.size
            else if (colsNum != r.size)
                assert(false, "Table with uneven rows.")

        assert(colsNum > 0)

        // At this point all rows in the table have the
        // the same number of columns.

        val colWs = new Array[Int](colsNum) // Column widths.
        val rowHs = new Array[Int](rows.length) // Row heights.

        // Header height.
        var hdrH = 0

        // Initialize column widths with header row (if any).
        for (i <- hdr.indices) {
            val c = hdr(i)

            colWs(i) = c.width

            hdrH = math.max(hdrH, c.height)
        }

        // Calc row heights and column widths.
        for (i <- rows.indices; j <- 0 until colsNum) {
            val c = rows(i)(j)

            rowHs(i) = math.max(rowHs(i), c.height)
            colWs(j) = math.max(colWs(j), c.width)
        }

        // Table width without the border.
        val tblW = colWs.sum + colsNum - 1

        val tbl = new GridStringBuilder()

        // Top margin.
        for (_ <- 0 until margin.top)
            tbl.a(" ").a(NL)

        // Print header, if any.
        if (isHdr) {
            tbl.a(blank(margin.left)).a(HDR_CRS).a(dash(HDR_HOR, tblW)).a(HDR_CRS).a(blank(margin.right)).a(NL)

            for (i <- 0 until hdrH) {
                // Left margin and '|'.
                tbl.a(blank(margin.left)).a(HDR_VER)

                for (j <- hdr.indices) {
                    val c = hdr(j)

                    if (i >= 0 && i < c.height)
                        tbl.a(aligned(c.lines(i), colWs(j), c.style))
                    else
                        tbl.a(blank(colWs(j)))

                    tbl.a(HDR_VER) // '|'
                }

                // Right margin.
                tbl.a(blank(margin.right)).a(NL)
            }

            tbl.a(blank(margin.left)).a(HDR_CRS).a(dash(HDR_HOR, tblW)).a(HDR_CRS).a(blank(margin.right)).a(NL)
        }
        else
            tbl.a(blank(margin.left)).a(ROW_CRS).a(dash(ROW_HOR, tblW)).a(ROW_CRS).a(blank(margin.right)).a(NL)

        // Print rows, if any.
        if (rows.nonEmpty) {
            val horLine = (i: Int) => {
                // Left margin and '+'
                tbl.a(blank(margin.left)).a(ROW_CRS)

                for (k <- rows(i).indices)
                    tbl.a(dash(ROW_HOR, colWs(k))).a(ROW_CRS)

                // Right margin.
                tbl.a(blank(margin.right)).a(NL)
            }

            for (i <- rows.indices) {
                val r = rows(i)

                val rowH = rowHs(i)

                if (i > 0 && ((rowH > 1 && autoBorder) || insideBorder) && rowHs(i - 1) == 1)
                    horLine(i)

                for (j <- 0 until rowH) {
                    // Left margin and '|'
                    tbl.a(blank(margin.left)).a(ROW_VER)

                    for (k <- r.indices) {
                        val c = r(k)
                        val w = colWs(k)

                        if (j < c.height)
                            tbl.a(aligned(c.lines(j), w, c.style))
                        else
                            tbl.a(blank(w))

                        tbl.a(ROW_VER) // '|'
                    }

                    // Right margin.
                    tbl.a(blank(margin.right)).a(NL)
                }

                if (i < rows.size - 1 && ((rowH > 1 && autoBorder) || insideBorder))
                    horLine(i)
            }

            tbl.a(blank(margin.left)).a(ROW_CRS).a(dash(ROW_HOR, tblW)).a(ROW_CRS).a(blank(margin.right)).a(NL)
        }

        // Bottom margin.
        for (_ <- 1 to margin.bottom)
            tbl.a(" ").a(NL)

        print(tbl.toString)
    }

    def nonEmpty = rows.nonEmpty
}

/**
 * Static context.
 */
object VisorTextTable {
    /** Table header horizontal line. */
    private val HDR_HOR = "="

    /** Table header vertical line. */
    private val HDR_VER = "|"

    /** Table header crossroad line. */
    private val HDR_CRS = "+"

    /** Table row horizontal line. */
    private val ROW_HOR = '-'

    /** Table row vertical line. */
    private val ROW_VER = '|'

    /** Table row crossroad line. */
    private val ROW_CRS = "+"

    /**
      * Creates new Visor text table.
      */
    def apply() =
        new VisorTextTable
}
