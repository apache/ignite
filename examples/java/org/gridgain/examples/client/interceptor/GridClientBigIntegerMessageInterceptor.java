// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.client.interceptor;

import org.gridgain.grid.*;
import org.gridgain.grid.product.*;
import org.jetbrains.annotations.*;

import java.math.*;
import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Example implementation of {@link GridClientMessageInterceptor}.
 * <p>
 * For demonstration purpose it converts received byte arrays to {@link BigInteger} and back.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridClientBigIntegerMessageInterceptor implements GridClientMessageInterceptor {
    /** {@inheritDoc} */
    @Override public Object onReceive(@Nullable Object obj) {
        if (obj instanceof byte[]) {
            System.out.println(">>> Byte array received over REST: " + Arrays.toString((byte[])obj));

            BigInteger val = new BigInteger((byte[])obj);

            System.out.println(">>> Unpacked a BigInteger from byte array received over REST: " + val);

            return val;
        }
        else
            return obj;
    }

    /** {@inheritDoc} */
    @Override public Object onSend(Object obj) {
        if (obj instanceof BigInteger) {
            System.out.println(">>> Creating byte array from BigInteger to send over REST: " + obj);

            byte[] bytes = ((BigInteger)obj).toByteArray();

            System.out.println(">>> Created byte array from BigInteger to send over REST: " + Arrays.toString(bytes));

            return bytes;
        }
        else
           return  obj;
    }
}
