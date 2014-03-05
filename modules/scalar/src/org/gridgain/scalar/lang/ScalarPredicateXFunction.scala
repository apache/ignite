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

import org.gridgain.grid.util.lang.GridPredicateX

/**
 * Wrapping Scala function for `GridPredicateX`.
 */
class ScalarPredicateXFunction[T](val inner: GridPredicateX[T]) extends (T => Boolean) {
    assert(inner != null)

    /**
     * Delegates to passed in grid predicate.
     */
    def apply(t: T) = {
        inner.applyx(t)
    }
}
