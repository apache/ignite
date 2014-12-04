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
import org.gridgain.grid.util.lang.IgnitePredicate2X

/**
 * Peer deploy aware adapter for Java's `GridPredicate2X`.
 */
class ScalarPredicate2X[T1, T2](private val p: (T1, T2) => Boolean) extends IgnitePredicate2X[T1, T2] {
    assert(p != null)

    /**
     * Delegates to passed in function.
     */
    @throws(classOf[GridException])
    def applyx(e1: T1, e2: T2): Boolean = {
        p(e1, e2)
    }
}
