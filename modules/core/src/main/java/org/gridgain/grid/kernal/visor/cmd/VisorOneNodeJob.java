/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

/**
 * Base class for Visor job intended to query data from a single node.
 *
 * @param <A> Task argument type.
 * @param <R> Task result type.
 */
public abstract class VisorOneNodeJob<A extends VisorOneNodeArg, R> extends VisorJob<A, R> {
    /**
     * Create job with specified argument.
     *
     * @param arg Job argument.
     */
    protected VisorOneNodeJob(A arg) {
        super(arg);
    }
}
