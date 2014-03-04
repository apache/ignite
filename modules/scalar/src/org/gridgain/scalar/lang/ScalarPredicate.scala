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

import org.gridgain.grid.lang.GridPredicate

/**
 * Peer deploy aware adapter for Java's `GridPredicate`.
 *
 * @author @java.author
 * @version @java.version
 */
class ScalarPredicate[T](private val p: T => Boolean) extends GridPredicate[T] {
    assert(p != null)

    /**
     * Delegates to passed in function.
     */
    def apply(e: T) = p(e)
}
