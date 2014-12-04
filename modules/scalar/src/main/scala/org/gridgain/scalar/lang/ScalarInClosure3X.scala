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
import org.gridgain.grid.util.lang.GridInClosure3X

/**
 * Peer deploy aware adapter for Java's `GridInClosure3X`.
 */
class ScalarInClosure3X[T1, T2, T3](private val f: (T1, T2, T3) => Unit) extends GridInClosure3X[T1, T2, T3] {
    assert(f != null)

    /**
     * Delegates to passed in function.
     */
    @throws(classOf[GridException])
    def applyx(t1: T1, t2: T2, t3: T3) {
        f(t1, t2, t3)
    }
}
