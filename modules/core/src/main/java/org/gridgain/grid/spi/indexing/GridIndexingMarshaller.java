/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import org.gridgain.grid.spi.*;

/**
 * Marshaller to be used in indexing SPI. This marshaller automatically
 * takes care of class loading of unmarshalled classes.
 * See also {@link GridIndexingSpi#registerMarshaller(GridIndexingMarshaller)}.
 */
public interface GridIndexingMarshaller {
    /**
     * Unmarshalls bytes to object.
     *
     * @param bytes Bytes.
     * @param <T> Value type.
     * @return Value.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public <T> GridIndexingEntity<T> unmarshal(byte[] bytes) throws IgniteSpiException;

    /**
     * Marshals object to bytes.
     *
     * @param entity Entity.
     * @return Bytes.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public byte[] marshal(GridIndexingEntity<?> entity) throws IgniteSpiException;
}
