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

import org.apache.ignite._
import org.gridgain.grid._
import org.gridgain.grid.util.lang.IgniteOutClosureX

/**
 * Peer deploy aware adapter for Java's `GridOutClosureX`.
 */
class ScalarOutClosureX[R](private val f: () => R) extends IgniteOutClosureX[R] {
    assert(f != null)

    /**
     * Delegates to passed in function.
     */
    @throws(classOf[IgniteCheckedException])
    def applyx(): R = {
        f()
    }
}
