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

import org.apache.ignite.lang.IgniteClosure

/**
 * Wrapping Scala function for `GridClosure`.
 */
class ScalarClosureFunction[T, R](val inner: IgniteClosure[T, R]) extends (T => R) {
    assert(inner != null)

    /**
     * Delegates to passed in grid closure.
     */
    def apply(t: T): R = {
        inner.apply(t)
    }
}
