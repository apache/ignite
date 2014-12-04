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

import org.gridgain.grid._
import org.gridgain.grid.util.lang.IgniteClosure2X

/**
 * Peer deploy aware adapter for Java's `GridClosure2X`.
 */
class ScalarClosure2X[E1, E2, R](private val f: (E1, E2) => R) extends IgniteClosure2X[E1, E2, R] {
    assert(f != null)

    /**
     * Delegates to passed in function.
     */
    @throws(classOf[GridException])
    def applyx(e1: E1, e2: E2): R = {
        f(e1, e2)
    }
}
