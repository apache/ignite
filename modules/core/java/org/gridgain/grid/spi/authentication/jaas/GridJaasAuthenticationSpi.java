/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication.jaas;

import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.authentication.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.security.auth.callback.*;
import javax.security.auth.login.*;

/**
 * JAAS-based implementation of the authentication SPI.
 * <p>
 * On authentication request this SPI delegates authentication to the externally configured
 * JAAS login module in accordance with <a
 * href="http://docs.oracle.com/javase/1.5.0/docs/guide/security/jaas/JAASRefGuide.html">
 * JAAS Reference Guide</a>:
 * <ol>
 *     <li>SPI creates new callback handler via configured {@link GridJaasCallbackHandlerFactory},
 *     which provides subject credentials in format acceptable by the JAAS login module.</li>
 *     <li>SPI passes context name and callback handler into the JAAS login context.</li>
 *     <li>JAAS login context delegates authentication to the JAAS login module specified by the context name.</li>
 *     <li>JAAS login module requests subject credentials from the callback handler and perform authentication.</li>
 * </ol>
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * The following configuration parameters are required:
 * <ul>
 *     <li>Callback handler's factory (see {@link #setCallbackHandlerFactory(GridJaasCallbackHandlerFactory)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 *     <li>JAAS login context name (see {@link #setLoginContextName(String)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * GridJaasAuthenticationSpi authSpi = getSpi();
 *
 * // Override JAAS login context name.
 * authSpi.setLoginContextName("GridJaasLoginContext");
 *
 * // Override callback handler's factory.
 * authSpi.setCallbackHandlerFactory(new GridJaasCallbackHandlerFactorySample());
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default authentication SPI.
 * cfg.setAuthenticationSpi(authSpi);
 *
 * // Start grid.
 * GridFactory.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridPasscodeAuthenticationSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="authenticationSpi"&gt;
 *             &lt;bean class="org.gridgain.grid.spi.authentication.jaas.GridJaasAuthenticationSpi"&gt;
 *                 &lt;property name="loginContextName" value="GridJaasLoginContext"/&gt;
 *                 &lt;property name="callbackHandlerFactory"&gt;
 *                     &lt;bean class="org.gridgain.grid.spi.authentication.jaas.GridJaasCallbackHandlerFactorySample"/&gt;
 *                 &lt;/property&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 *
 * @author @java.author
 * @version @java.version
 */
@GridSpiInfo(
    author = /*@java.spi.author*/"GridGain Systems",
    url = /*@java.spi.url*/"www.gridgain.com",
    email = /*@java.spi.email*/"support@gridgain.com",
    version = /*@java.spi.version*/"x.x")
@GridSpiMultipleInstancesSupport(true)
public class GridJaasAuthenticationSpi extends GridSpiAdapter
    implements GridAuthenticationSpi, GridJaasAuthenticationSpiMBean {
    /** Injected grid logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Login context name. */
    private String loginCtxName = "GridJaasLoginContext";

    /** Callback handlers factory. */
    private GridJaasCallbackHandlerFactory callbackHndFactory;

    /** {@inheritDoc} */
    @Override public String getLoginContextName() {
        return loginCtxName;
    }

    /** {@inheritDoc} */
    @GridSpiConfiguration(optional = true)
    @Override public void setLoginContextName(String loginCtxName) {
        assert loginCtxName != null;
        this.loginCtxName = loginCtxName;
    }

    /** {@inheritDoc} */
    @Override public String getCallbackHandlerFactoryFormatted() {
        return callbackHndFactory.toString();
    }

    /**
     * Sets JAAS-implementation specific callback handler factory.
     *
     * @param callbackHndFactory JAAS-implementation specific callback handler factory.
     */
    @GridSpiConfiguration(optional = false)
    public void setCallbackHandlerFactory(GridJaasCallbackHandlerFactory callbackHndFactory) {
        this.callbackHndFactory = callbackHndFactory;
    }

    /** {@inheritDoc} */
    @Override public boolean authenticate(GridSecuritySubjectType subjType, byte[] subjId,
        @Nullable Object creds) throws GridSpiException {
        CallbackHandler cbHnd = callbackHndFactory.newInstance(subjType, subjId, creds);

        if (cbHnd == null)
            return false;

        LoginContext lc;

        try {
            lc = new LoginContext(loginCtxName, cbHnd);
        }
        catch (LoginException e) {
            throw new GridSpiException("Failed to create login context: " + loginCtxName, e);
        }

        try {
            lc.login();
        }
        catch (LoginException ignore) {
            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supported(GridSecuritySubjectType subjType) {
        return callbackHndFactory.supported(subjType);
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        assertParameter(callbackHndFactory != null, "callbackHandlerFactory != null");

        // Start SPI start stopwatch.
        startStopwatch();

        registerMBean(gridName, this, GridJaasAuthenticationSpiMBean.class);

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
        return S.toString(GridJaasAuthenticationSpi.class, this);
    }
}
