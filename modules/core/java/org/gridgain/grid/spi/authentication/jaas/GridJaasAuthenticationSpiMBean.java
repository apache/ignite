// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication.jaas;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;

/**
 * Management bean for {@link GridJaasAuthenticationSpi}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridMBeanDescription("MBean that provides access to Jaas-based authentication SPI configuration.")
public interface GridJaasAuthenticationSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets login context name.
     *
     * @return Login context name.
     */
    @GridMBeanDescription("Login context name.")
    public String getLoginContextName();

    /**
     * Sets new login context name.
     *
     * @param loginCtxName New login context name.
     */
    @GridMBeanDescription("Sets login context name.")
    public void setLoginContextName(String loginCtxName);

    /**
     * Gets JAAS-authentication callback handler factory name.
     *
     * @return JAAS-authentication callback handler factory name.
     */
    @GridMBeanDescription("String presentation of JAAS-authentication callback handler factory.")
    public String getCallbackHandlerFactoryFormatted();
}
