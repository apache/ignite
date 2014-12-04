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

import org.apache.ignite.lang.IgniteRunnable
import org.gridgain.grid.util.lang.GridPeerDeployAwareAdapter

/**
 * Peer deploy aware adapter for Java's `GridRunnable`.
 */
class ScalarAbsClosure(private val f: () => Unit) extends GridPeerDeployAwareAdapter with IgniteRunnable {
    assert(f != null)

    peerDeployLike(f)

    /**
     * Delegates to passed in function.
     */
    def run() {
        f()
    }
}
