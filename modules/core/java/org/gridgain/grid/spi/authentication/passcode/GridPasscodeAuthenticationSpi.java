// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication.passcode;

import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.authentication.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.spi.GridSecuritySubjectType.*;

/**
 * Passcode-based implementation of the authentication SPI.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 *     <li>Authentication passcodes map (see {@link #setPasscodes(Map)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * GridPasscodeAuthenticationSpi authSpi = new GridPasscodeAuthenticationSpi();
 *
 * // Override authentication passcode.
 * authSpi.setPasscodes(F.asMap(REMOTE_NODE, "s3cret", REMOTE_CLIENT, "s3cret"));
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
 *             &lt;bean class="org.gridgain.grid.spi.authentication.passcode.GridPasscodeAuthenticationSpi"&gt;
 *                 &lt;!-- Override passcodes. --&gt;
 *                 &lt;property name="passcodes"&gt;
 *                     &lt;map&gt;
 *                         &lt;entry key="REMOTE_NODE" value="s3cret"/&gt;
 *                         &lt;entry key="REMOTE_CLIENT" value="s3cret"/&gt;
 *                     &lt;/map&gt;
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
@GridSpiConsistencyChecked(optional = false)
public class GridPasscodeAuthenticationSpi extends GridSpiAdapter
    implements GridAuthenticationSpi, GridPasscodeAuthenticationSpiMBean {
    /** Authentication passcode node attribute key should be the same on all nodes. */
    static final String NODE_PASSCODE_ATTRIBUTE_KEY = "auth.passcode";

    /** Default passcodes collection. */
    private static final Map<GridSecuritySubjectType, String> DEFAULT_PASSCODES =
        Collections.unmodifiableMap(F.asMap(REMOTE_CLIENT, "", REMOTE_NODE, ""));

    /** Collection of valid passcodes. */
    private Map<GridSecuritySubjectType, String> passcodes = DEFAULT_PASSCODES;

    /** Injected grid logger. */
    @GridLoggerResource
    private GridLogger log;

    /**
     * Sets valid authentication passcodes.
     *
     * @param passcodes Valid authentication passcodes.
     */
    @GridSpiConfiguration(optional = true)
    public void setPasscodes(Map<GridSecuritySubjectType, String> passcodes) {
        A.ensure(passcodes != null, "passcodes != null");

        this.passcodes = passcodes;
    }

    /** {@inheritDoc} */
    @Override public String getPasscodesFormatted() {
        StringBuilder builder = new StringBuilder("Passcodes: [");
        String separator = "";

        for (Map.Entry<GridSecuritySubjectType, String> entry : passcodes.entrySet()) {
            // Append passcode entry.
            builder.append(separator).append(entry.getKey()).append("='").append(entry.getValue()).append("'");

            // Update passcode entries separator.
            separator = ", ";
        }

        return builder.append("]").toString();
    }

    /** {@inheritDoc} */
    @Override public boolean supported(GridSecuritySubjectType subjType) {
        assert subjType != null;

        return passcodes.containsKey(subjType);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IfMayBeConditional")
    @Override public boolean authenticate(GridSecuritySubjectType subjType, byte[] subjId,
        @Nullable Object creds) throws GridSpiException {
        // Validate input parameters.
        assert subjType != null;
        assert subjId != null;
        assert supported(subjType) : "Unsupported subject type: " + subjType;

        if (creds instanceof Map)
            // Get passcode from the node attribute.
            creds = ((Map)creds).get(createSpiAttributeName(NODE_PASSCODE_ATTRIBUTE_KEY));

        String passcode = null;

        if (creds instanceof String)
            // Interprets credentials as passcode.
            passcode = (String)creds;

        // Check credentials is a string configured in the SPI passcodes.
        return passcode != null && passcode.equals(passcodes.get(subjType));
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws GridSpiException {
        String passcode = passcodes.get(REMOTE_NODE);

        if (passcode != null)
            return F.<String, Object>asMap(createSpiAttributeName(NODE_PASSCODE_ATTRIBUTE_KEY), passcode);

        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        // Check passcodes are configured.
        assertParameter(passcodes != null, "passcodes != null");

        // Start SPI start stopwatch.
        startStopwatch();

        registerMBean(gridName, this, GridPasscodeAuthenticationSpiMBean.class);

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
        return S.toString(GridPasscodeAuthenticationSpi.class, this);
    }
}
