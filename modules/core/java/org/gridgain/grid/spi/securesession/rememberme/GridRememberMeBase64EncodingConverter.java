// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.securesession.rememberme;

import org.apache.commons.codec.binary.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Base64 encoder.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridRememberMeBase64EncodingConverter implements GridRememberMeConverter {
    /** {@inheritDoc} */
    @Override public byte[] convert(byte[] data) {
        return Base64.encodeBase64(data);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRememberMeBase64EncodingConverter.class, this);
    }
}
