/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.marshaller;

import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.io.*;
import org.jetbrains.annotations.*;

/**
 * Base class for marshallers. Provides default implementations of methods
 * that work with byte array or {@link GridByteArrayList}. These implementations
 * use {@link GridByteArrayInputStream} or {@link GridByteArrayOutputStream}
 * to marshal and unmarshal objects.
 */
public abstract class IgniteAbstractMarshaller implements IgniteMarshaller {
    /** Default initial buffer size for the {@link GridByteArrayOutputStream}. */
    public static final int DFLT_BUFFER_SIZE = 512;

    /** {@inheritDoc} */
    @Override public byte[] marshal(@Nullable Object obj) throws GridException {
        GridByteArrayOutputStream out = null;

        try {
            out = new GridByteArrayOutputStream(DFLT_BUFFER_SIZE);

            marshal(obj, out);

            return out.toByteArray();
        }
        finally {
            U.close(out, null);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] arr, @Nullable ClassLoader clsLdr) throws GridException {
        GridByteArrayInputStream in = null;

        try {
            in = new GridByteArrayInputStream(arr, 0, arr.length);

            return unmarshal(in, clsLdr);
        }
        finally {
            U.close(in, null);
        }
    }
}
