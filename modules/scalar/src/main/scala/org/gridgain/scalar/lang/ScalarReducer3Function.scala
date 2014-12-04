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

import org.gridgain.grid.util.lang.{IgniteReducer3}

/**
 * Wrapping Scala function for `GridReducer3`.
 */
class ScalarReducer3Function[E1, E2, E3, R](val inner: IgniteReducer3[E1, E2, E3, R]) extends
    ((Seq[E1], Seq[E2], Seq[E3]) => R) {
    assert(inner != null)

    /**
     * Delegates to passed in grid reducer.
     */
    def apply(s1: Seq[E1], s2: Seq[E2], s3: Seq[E3]) = {
        for (e1 <- s1; e2 <- s2; e3 <- s3) inner.collect(e1, e2, e3)

        inner.apply()
    }
}
