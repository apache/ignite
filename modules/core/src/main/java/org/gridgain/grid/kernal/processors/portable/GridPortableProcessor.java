/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

import org.gridgain.client.marshaller.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.portable.*;
import org.jetbrains.annotations.*;

/**
 * Portable processor.
 */
public interface GridPortableProcessor extends GridProcessor {
    /**
     * Whether portable marshaller is configured.
     *
     * @return Whether portable marshaller is configured.
     */
    public boolean isPortableEnabled();

    /**
     * Properly initializes configuration of portable marshaller for client connectivity.
     *
     * @param marsh Marshaller.
     */
    public void configureClientMarshaller(GridClientMarshaller marsh);

    /**
     * @param obj Object.
     * @return Whether object is portable.
     */
    public boolean isPortable(@Nullable Object obj) throws GridPortableException;

    /**
     * @param typeId Type ID.
     * @return Type name.
     */
    public String typeName(int typeId);

    /**
     * @param obj Object to marshal.
     * @return Portable object.
     * @throws GridPortableException In case of error.
     */
    public <T> GridPortableObject<T> marshal(@Nullable T obj) throws GridPortableException;

    /**
     * @param arr Byte array.
     * @return Portable object.
     * @throws GridPortableException
     */
    @Nullable public <T> T unmarshal(byte[] arr) throws GridPortableException;
}
