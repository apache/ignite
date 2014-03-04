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

import java.util.concurrent.Callable

/**
 * Wrapping Scala function for `Callable` and specifically for `GridOutClosure`.
 *
 * @author @java.author
 * @version @java.version
 */
class ScalarOutClosureFunction[R](val inner: Callable[R]) extends (() => R) {
    assert(inner != null)

    /**
     * Delegates to passed in grid closure.
     */
    def apply(): R =
        inner.call()
}
