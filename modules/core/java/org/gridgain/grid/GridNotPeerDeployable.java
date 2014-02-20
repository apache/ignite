// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import java.lang.annotation.*;

/**
 * This annotations should be used to mark any type that should not be
 * peer deployable. Peer deployment will fail for this object as if
 * class could not be found.
 * <p>
 * This annotation is used as <b>non-distribution assertion</b> and should be
 * applied to classes and interfaces that should never be distributed via
 * peer-to-peer deployment.
 * <p>
 * Note, however, that if class is already available on the remote node it
 * will not be peer-loaded but will simply be locally class loaded. It may appear
 * as if it was successfully peer-loaded when in fact it was simply already
 * available on the remote node.
 *
 * @author @java.author
 * @version @java.version
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GridNotPeerDeployable {
    // No-op.
}
