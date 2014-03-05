/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.securesession.rememberme;

import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.securesession.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.spi.GridSecuritySubjectType.*;
import static org.gridgain.grid.util.GridUtils.*;

/**
 * Remember-me implementation of the secure session SPI supports only remote client as a subject type.
 * It doesn't store any session information itself, but encode it inside the token.
 * <p>
 * It generates a secure session token with encapsulated session data and data signature, encrypted with secret key.
 * On request this SPI validates received token with secret key and re-generate new token.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 *     <li>Session token expiration time (see {@link #setTtl(long)})</li>
 *     <li>Secret key to sign session data or verify its integrity (see {@link #setSecretKey(String)})</li>
 *     <li>Hash generator to sign session data with secret key (see {@link #setSigner(GridRememberMeConverter)})</li>
 *     <li>Session token encoder (see {@link #setEncoder(GridRememberMeConverter)})</li>
 *     <li>Session token decoder (see {@link #setDecoder(GridRememberMeConverter)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * GridRememberMeSecureSessionSpi spi = new GridRememberMeSecureSessionSpi();
 *
 * // Set session expiration time for 1 hour.
 * spi.setTtl(60 * 60 * 1000);
 *
 * // Set data signature secret key.
 * spi.setSecretKey("1001 hedgehogs");
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default SecureSession SPI.
 * cfg.setSecureSessionSpi(spi);
 *
 * // Start grid.
 * GridGain.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridRememberMeSecureSessionSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="secureSessionSpi"&gt;
 *             &lt;bean class="org.gridgain.grid.spi.SecureSession.rememberme.GridRememberMeSecureSessionSpi"&gt;
 *                 &lt;property name="ttl" value="3600000"/&gt;
 *                 &lt;property name="secretKey" value="1001 hedgehogs"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridSecureSessionSpi
 */
@GridSpiInfo(
    author = /*@java.spi.author*/"GridGain Systems",
    url = /*@java.spi.url*/"www.gridgain.com",
    email = /*@java.spi.email*/"support@gridgain.com",
    version = /*@java.spi.version*/"x.x")
@GridSpiMultipleInstancesSupport(true)
public class GridRememberMeSecureSessionSpi extends GridSpiAdapter
    implements GridSecureSessionSpi, GridRememberMeSecureSessionSpiMBean {
    /** Default character set for secret key. */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /** Empty bytes array. */
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /** Injected grid logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Session token encoder. */
    private GridRememberMeConverter encoder = new GridRememberMeBase64EncodingConverter();

    /** Session token decoder. */
    private GridRememberMeConverter decoder = new GridRememberMeBase64DecodingConverter();

    /** Data signer to verify session token integrity. */
    private GridRememberMeConverter signer = new GridRememberMeSigningConverter();

    /** Data signer secret key. */
    private byte[] secretKey = EMPTY_BYTE_ARRAY;

    /** Session time to live in milliseconds. */
    private long ttl = TimeUnit.MINUTES.toMillis(60);

    /**
     * Set new session token encoder.
     *
     * @param encoder New session token encoder.
     */
    @GridSpiConfiguration(optional = true)
    public void setEncoder(GridRememberMeConverter encoder) {
        this.encoder = encoder;
    }

    /** {@inheritDoc} */
    @Override public String getEncoderFormatted() {
        return encoder == null ? "" : encoder.toString();
    }

    /**
     * Set new session token decoder.
     *
     * @param decoder New session token decoder.
     */
    @GridSpiConfiguration(optional = true)
    public void setDecoder(GridRememberMeConverter decoder) {
        this.decoder = decoder;
    }

    /** {@inheritDoc} */
    @Override public String getDecoderFormatted() {
        return decoder == null ? "" : decoder.toString();
    }

    /**
     * Set new data signer to sign/verify session data integrity.
     *
     * @param signer New data signer to sign/verify session data integrity.
     */
    @GridSpiConfiguration(optional = true)
    public void setSigner(GridRememberMeConverter signer) {
        this.signer = signer;
    }

    /** {@inheritDoc} */
    @Override public String getSignerFormatted() {
        return signer == null ? "" : signer.toString();
    }

    /** {@inheritDoc} */
    @Override public String getSecretKey() {
        return new String(secretKey, UTF_8);
    }

    /** {@inheritDoc} */
    @GridSpiConfiguration(optional = true)
    @Override public void setSecretKey(String secretKey) {
        this.secretKey = secretKey == null ? EMPTY_BYTE_ARRAY : secretKey.getBytes(UTF_8);
    }

    /** {@inheritDoc} */
    @Override public long getTtl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @GridSpiConfiguration(optional = true)
    @Override public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    /** {@inheritDoc} */
    @Override public boolean supported(GridSecuritySubjectType subjType) {
        // Only remote clients can use "remember-me" sessions.
        return REMOTE_CLIENT == subjType;
    }

    /** {@inheritDoc} */
    @Override public byte[] validate(GridSecuritySubjectType subjType, byte[] subjId, @Nullable byte[] tok,
        @Nullable Object params) throws GridSpiException {
        assert subjType != null;
        assert subjId != null;
        assert supported(subjType) : "Unsupported subject type: " + subjType;

        // Create token for new or valid session.
        return tok == null || isValidToken(subjId, tok) ? createToken(subjId) : null;
    }

    /**
     * Create token for specified subject ID.
     *
     * @param subjId Unique subject ID such as local or remote node ID, client ID, etc.
     * @return New token.
     * @throws GridSpiException If token creation resulted in system error.
     */
    protected byte[] createToken(byte[] subjId) throws GridSpiException {
        // Token format:
        // encode(createTime:signature)
        //
        // E.g.:
        // base64(createTime:sha1(subjId:createTime:secretKey))

        // Token creation time.
        long createTime = U.currentTimeMillis();

        // Calculate signature.
        byte[] sign = sign(subjId, createTime);

        // Concatenate token data and signature.
        byte[] cnct = join(longToBytes(createTime), sign);

        // Generate encoded session token, e.g. base64-encoded.
        return encoder.convert(cnct);
    }

    /**
     * Validate session token against subject ID.
     *
     * @param subjId Unique subject ID such as local or remote node ID, client ID, etc.
     * @param tok Session token to validate.
     * @return Returns {@code true} if session token is valid, {@code false} - otherwise.
     * @throws GridSpiException If session validation resulted in system error.
     */
    protected boolean isValidToken(byte[] subjId, byte[] tok) throws GridSpiException {
        // Decode session token into concatenated data.
        byte[] cnct = decoder.convert(tok);

        if (cnct == null)
            return false;

        // Recover token creation time.
        long createTime = bytesToLong(cnct, 0);

        // Validate token expiration. Ignore expiration if TTL is set to zero.
        long ttl = getTtl();

        if (ttl != 0 && createTime + ttl < U.currentTimeMillis())
            return false; // Token expired.

        // Recover stored token signature.
        byte[] sign = Arrays.copyOfRange(cnct, 8 /* size of createTime */, cnct.length);

        // Generate valid token signature.
        byte[] verification = sign(subjId, createTime);

        // Validate stored and valid token signatures.
        return Arrays.equals(sign, verification);
    }

    /**
     * Sign data.
     *
     * @param subjId Unique subject ID such as local or remote node ID, client ID, etc.
     * @param createTime Token creation time.
     * @return Data signature.
     * @throws GridSpiException If data signing resulted in system error.
     */
    protected byte[] sign(byte[] subjId, long createTime) throws GridSpiException {
        byte[] sign = signer.convert(join(subjId, longToBytes(createTime), secretKey));

        if (sign == null)
            sign = EMPTY_BYTE_ARRAY;

        return sign;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        // Check configuration.
        assertParameter(encoder != null, "encoder != null");
        assertParameter(decoder != null, "decoder != null");
        assertParameter(signer != null, "signer != null");
        assertParameter(ttl >= 0, "ttl >= 0");

        // Start SPI start stopwatch.
        startStopwatch();

        registerMBean(gridName, this, GridRememberMeSecureSessionSpiMBean.class);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRememberMeSecureSessionSpi.class, this);
    }
}
