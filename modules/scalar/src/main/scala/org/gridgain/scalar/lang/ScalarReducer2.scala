/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.lang

import collection._
import org.gridgain.grid.util.lang.IgniteReducer2

/**
 * Peer deploy aware adapter for Java's `GridReducer2`.
 */
class ScalarReducer2[E1, E2, R](private val r: (Seq[E1], Seq[E2]) => R) extends IgniteReducer2[E1, E2, R] {
    assert(r != null)

    private val buf1 = new mutable.ListBuffer[E1]
    private val buf2 = new mutable.ListBuffer[E2]

    /**
     * Delegates to passed in function.
     */
    def apply = r(buf1.toSeq, buf2.toSeq)

    /**
     * Collects given values.
     *
     * @param e1 Value to collect for later reduction.
     * @param e2 Value to collect for later reduction.
     */
    def collect(e1: E1, e2: E2) = {
        buf1 += e1
        buf2 += e2

        true
    }
}
