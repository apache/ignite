/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.securesession.rememberme;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * No-op converter - {@link #convert(byte[])} method returns the same byte array as
 * the passed-in one.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridRememberMePassThroughConverter implements GridRememberMeConverter {
    /** {@inheritDoc} */
    @Override public byte[] convert(byte[] data) throws GridSpiException {
        return data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRememberMePassThroughConverter.class, this);
    }
}
