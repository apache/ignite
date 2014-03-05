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

import org.gridgain.grid.util.lang.{GridLambdaAdapter, GridClosureX}

/**
 * Wrapping Scala function for `GridClosureX`.
 */
class ScalarClosureXFunction[T, R](val inner: GridClosureX[T, R]) extends GridLambdaAdapter with (T => R) {
    assert(inner != null)

    peerDeployLike(inner)

    /**
     * Delegates to passed in grid closure.
     */
    def apply(t: T): R = {
        inner.applyx(t)
    }
}
