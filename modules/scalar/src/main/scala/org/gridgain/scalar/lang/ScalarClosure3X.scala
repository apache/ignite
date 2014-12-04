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
import org.gridgain.grid.util.lang.GridClosure3X

/**
 * Peer deploy aware adapter for Java's `GridClosure3X`.
 */
class ScalarClosure3X[E1, E2, E3, R](private val f: (E1, E2, E3) => R) extends GridClosure3X[E1, E2, E3, R] {
    assert(f != null)

    /**
     * Delegates to passed in function.
     */
    @throws(classOf[GridException])
    def applyx(e1: E1, e2: E2, e3: E3): R = {
        f(e1, e2, e3)
    }
}
