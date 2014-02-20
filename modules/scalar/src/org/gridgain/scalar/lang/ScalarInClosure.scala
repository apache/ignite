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

import org.gridgain.grid.lang.GridInClosure

/**
 * Peer deploy aware adapter for Java's `GridInClosure`.
 *
 * @author @java.author
 * @version @java.version
 */
class ScalarInClosure[T](private val f: T => Unit) extends GridInClosure[T] {
    assert(f != null)

    peerDeployLike(f)

    /**
     * Delegates to passed in function.
     */
    def apply(t: T) {
        f(t)
    }
}
