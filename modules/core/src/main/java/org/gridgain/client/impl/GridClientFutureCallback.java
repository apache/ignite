/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl;

import org.gridgain.client.*;

/**
 * Future callback will be notified, when listened future finishes (both succeed or failed).
 * @param <R> Input parameter type.
 * @param <S> Result type.
 */
public interface GridClientFutureCallback<R, S> {
    /**
     * Future callback to executed when listened future finishes.
     *
     * @param fut Finished future to listen for.
     * @return Chained future result, if applicable, otherwise - {@code null}.
     */
    public S onComplete(GridClientFuture<R> fut) throws GridClientException;
}
