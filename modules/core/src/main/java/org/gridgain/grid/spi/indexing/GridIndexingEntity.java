/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Wrapper around indexed key or value which also may contain the value in
 * unmarshalled form. It exists to avoid unnecessary unmarshalling whenever
 * it is not needed.
 * See also {@link GridIndexingSpi#queryFields(String, String, Collection, GridIndexingQueryFilter[])}.
 */
public interface GridIndexingEntity<T> {
    /**
     * Gets indexed value. This method may return {@code null} only
     * if actual value is {@code null}. Otherwise, it will unmarshal
     * the {@link #bytes()} and return the actual value.
     *
     * @return Indexed value.
     * @throws org.gridgain.grid.spi.IgniteSpiException If value de-serialization failed.
     */
    @Nullable public T value() throws IgniteSpiException;

    /**
     * Optional bytes for marshaled indexed value. Certain SPI implementations
     * may keep objects in unmarshalled form and therefore will not provide
     * marshaled bytes for them.
     *
     * @return Optional marshaled value.
     */
    @Nullable public byte[] bytes();

    /**
     * Flag indicating whether this entity contains unmarshalled value.
     *
     * @return {@code True} if entity contains unmarshalled value, {@code false}
     *      otherwise.
     */
    public boolean hasValue();
}
