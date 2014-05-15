/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.securesession.rememberme;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.xml.bind.*;
import java.io.*;

/**
 * Base64 decoder.
 */
public class GridRememberMeBase64DecodingConverter implements GridRememberMeConverter {
    /** {@inheritDoc} */
    @Override public byte[] convert(byte[] data) {
        try {
            return DatatypeConverter.parseBase64Binary(new String(data, "UTF-8"));
        }
        catch (UnsupportedEncodingException e) {
            throw new GridRuntimeException(e);
        }
        catch (Exception ignored) {
            return null; // Invalid Base64 data.
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRememberMeBase64DecodingConverter.class, this);
    }
}
