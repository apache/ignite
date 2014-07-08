/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

import org.gridgain.grid.*;
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
     * Properly initializes configuration for client connectivity.
     *
     * @param cfg Marshaller.
     */
    public void configureClientConnection(GridClientConnectionConfiguration cfg);

    /**
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName);

    /**
     * @param obj Object to marshal.
     * @return Portable object.
     * @throws GridPortableException In case of error.
     */
    public Object marshalToPortable(@Nullable Object obj) throws GridPortableException;
}
