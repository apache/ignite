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

import org.gridgain.grid.lang.{GridPredicate}
import org.gridgain.grid.util.lang.GridLambdaAdapter

/**
 * Wrapping Scala function for `GridPredicate`.
 *
 * @author @java.author
 * @version @java.version
 */
class ScalarPredicateFunction[T](val inner: GridPredicate[T]) extends GridLambdaAdapter with (T => Boolean) {
    assert(inner != null)

    peerDeployLike(inner)

    /**
     * Delegates to passed in grid predicate.
     */
    def apply(t: T): Boolean = {
        inner.apply(t)
    }
}
