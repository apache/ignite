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
 * Peer deploy aware adapter for Java's `GridClosure`.
 */
class ScalarClosure[E, R](private val f: E => R) extends IgniteClosure[E, R] {
    assert(f != null)

    /**
     * Delegates to passed in function.
     */
    def apply(e: E): R = {
        f(e)
    }
}
