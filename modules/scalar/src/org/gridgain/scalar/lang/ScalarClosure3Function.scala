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

import org.gridgain.grid.util.lang.{GridLambdaAdapter, GridClosure3}

/**
 * Wrapping Scala function for `GridClosure3`.
 *
 * @author @java.author
 * @version @java.version
 */
class ScalarClosure3Function[T1, T2, T3, R](val inner: GridClosure3[T1, T2, T3, R]) extends GridLambdaAdapter
    with ((T1, T2, T3) => R) {
    assert(inner != null)

    peerDeployLike(inner)

    /**
     * Delegates to passed in grid closure.
     */
    def apply(t1: T1, t2: T2, t3: T3): R = {
        inner.apply(t1, t2, t3)
    }
}
