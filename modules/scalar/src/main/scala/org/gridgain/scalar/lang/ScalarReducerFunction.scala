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

import org.gridgain.grid.lang.{IgniteReducer}

/**
 * Wrapping Scala function for `GridReducer`.
 */
class ScalarReducerFunction[E1, R](val inner: IgniteReducer[E1, R]) extends (Seq[E1] => R) {
    assert(inner != null)

    /**
     * Delegates to passed in grid reducer.
     */
    def apply(s: Seq[E1]) = {
        s foreach inner.collect _

        inner.reduce()
    }
}
