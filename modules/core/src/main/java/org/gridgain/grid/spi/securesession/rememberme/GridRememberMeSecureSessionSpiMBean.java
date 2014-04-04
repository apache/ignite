/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.securesession.rememberme;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;

/**
 * Management bean for {@link GridRememberMeSecureSessionSpi}.
 */
@GridMBeanDescription("MBean that provides access to remember-me secure session SPI configuration.")
public interface GridRememberMeSecureSessionSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets session signature secret key used for signing and validation.
     *
     * @return Session token signature secret key.
     */
    @GridMBeanDescription("Secret key to sign and validate session token data.")
    public String getSecretKey();

    /**
     * Sets session signature secret key used for signing and validation.
     *
     * @param secretKey new session token signature secret key.
     */
    @GridMBeanDescription("Set secret key to sign and validate session token data.")
    public void setSecretKey(String secretKey);

    /**
     * Gets signer string representation.
     *
     * @return Signer string representation.
     */
    @GridMBeanDescription("Gets signer string representation.")
    public String getSignerFormatted();

    /**
     * Gets encoder string representation.
     *
     * @return Encoder string representation.
     */
    @GridMBeanDescription("Gets encoder string representation.")
    public String getEncoderFormatted();

    /**
     * Gets decoder string representation.
     *
     * @return Decoder string representation.
     */
    @GridMBeanDescription("Gets decoder string representation.")
    public String getDecoderFormatted();

    /**
     * Get session token expiration time (in milliseconds)
     *
     * @return Session token expiration time (in milliseconds)
     */
    @GridMBeanDescription("Session token expiration time in milliseconds (time-to-live).")
    public long getTtl();

    /**
     * Sets session expiration time (in milliseconds).
     *
     * @param ttl New session token expiration time (in milliseconds)
     */
    @GridMBeanDescription("Set session token expiration time in milliseconds (time-to-live).")
    public void setTtl(long ttl);
}
