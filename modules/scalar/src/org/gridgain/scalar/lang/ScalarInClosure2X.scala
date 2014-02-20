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

import org.gridgain.grid.lang._
import org.gridgain.grid._
import org.gridgain.grid.util.lang.GridInClosure2X

/**
 * Peer deploy aware adapter for Java's `GridInClosure2X`.
 *
 * @author @java.author
 * @version @java.version
 */
class ScalarInClosure2X[T1, T2](private val f: (T1, T2) => Unit) extends GridInClosure2X[T1, T2] {
    assert(f != null)

    peerDeployLike(f)

    /**
     * Delegates to passed in function.
     */
    @throws(classOf[GridException])
    def applyx(t1: T1, t2: T2) {
        f(t1, t2)
    }
}
