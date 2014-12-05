/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.jetbrains.annotations.*;

/**
 * Interface for user-defined object interceptors.
 * <p>
 * Interceptors allow user to transform objects send and received via REST protocols.
 * For example they could be used for customized multi-language marshalling by
 * converting binary object representation received from client to java object.
 */
public interface ClientMessageInterceptor {
    /**
     * Intercepts received objects.
     *
     * @param obj Original incoming object.
     * @return Object which should replace original in later processing.
     */
    @Nullable public Object onReceive(@Nullable Object obj);

    /**
     * Intercepts received objects.
     *
     * @param obj Original incoming object.
     * @return Object which should be send to remote client instead of original.
     */
    @Nullable public Object onSend(Object obj);
}
