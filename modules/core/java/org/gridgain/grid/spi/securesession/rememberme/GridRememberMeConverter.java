/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.securesession.rememberme;

import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

/**
 * Converter from one byte format to another.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridRememberMeConverter {
    /**
     * Convert binary message from one format to another.
     *
     * @param data Binary data to process with this message digest.
     * @return processed data or {@code null} if data cannot be processed
     * @throws GridSpiException Thrown on any system exception. Note: this method should return {@code
     * null} value, if passed in data cannot be processed.
     */
    @Nullable byte[] convert(byte[] data) throws GridSpiException;
}
