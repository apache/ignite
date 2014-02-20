// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.securesession.rememberme;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.security.*;

/**
 * Binary data signer based on the java security {@link MessageDigest}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridRememberMeSigningConverter implements GridRememberMeConverter {
    /** Hashing algorithm. */
    private String algo = "SHA1";

    /** Security provider to get hashing algorithm implementation from. */
    private String provider;

    /**
     * Set new hashing algorithm, e.g. "SHA-256".
     *
     * @param algo New hashing algorithm for data filtering.
     */
    public void setAlgorithm(String algo) {
        this.algo = algo;
    }

    /**
     * Sen new security provider, e.g. "BC" for Bouncy Castle implementation.
     *
     * @param provider New security provider.
     */
    public void setProvider(String provider) {
        this.provider = provider;
    }

    /** {@inheritDoc} */
    @Override public byte[] convert(byte[] data) throws GridSpiException {
        try {
            MessageDigest digest = provider == null ?
                MessageDigest.getInstance(algo) : MessageDigest.getInstance(algo, provider);

            return digest.digest(data);
        }
        catch (NoSuchAlgorithmException e) {
            throw new GridSpiException("Invalid hashing algorithm: " + algo, e);
        }
        catch (NoSuchProviderException e) {
            throw new GridSpiException("Invalid security provider: " + provider, e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRememberMeSigningConverter.class, this);
    }
}
