// @scala.file.header

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.lang

import org.gridgain.grid.lang.GridMapper
import org.gridgain.scalar._
import scalar._
import org.gridgain.grid._

/**
 * Peer deploy aware adapter for Java's `GridMapper`.
 *
 * @author @java.author
 * @version @java.version
 */
class ScalarMapper[T1, T2 >: GridNode](private val f: Seq[T2] => (T1 => T2)) extends GridMapper[T1, T2] {
    assert(f != null)

    peerDeployLike(f)

    private var p: T1 => T2 = null

    /**
     * Delegates to passed in function.
     */
    def apply(e: T1) = {
        assert(p != null)

        p(e)
    }

    /**
     * Collects values for later mapping.
     *
     * @param vals Values to collect.
     */
    def collect(vals: java.util.Collection[T2]) {
        assert(vals != null)

        p = f(toScalaSeq(vals))
    }
}
