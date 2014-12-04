/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.authentication.*;
import org.gridgain.grid.spi.securesession.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.io.*;
import java.nio.*;
import java.text.*;
import java.util.*;

import static org.gridgain.grid.GridSystemProperties.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * This class provides convenient adapter for SPI implementations.
 */
public abstract class GridSpiAdapter implements GridSpi, GridSpiManagementMBean {
    /** */
    private ObjectName spiMBean;

    /** SPI start timestamp. */
    private long startTstamp;

    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** */
    @IgniteMBeanServerResource
    private MBeanServer jmx;

    /** */
    @IgniteHomeResource
    private String ggHome;

    /** */
    @IgniteLocalNodeIdResource
    private UUID nodeId;

    /** SPI name. */
    private String name;

    /** Grid SPI context. */
    private volatile GridSpiContext spiCtx = new GridDummySpiContext(null);

    /** Discovery listener. */
    private GridLocalEventListener paramsLsnr;

    /**
     * Creates new adapter and initializes it from the current (this) class.
     * SPI name will be initialized to the simple name of the class
     * (see {@link Class#getSimpleName()}).
     */
    protected GridSpiAdapter() {
        name = U.getSimpleName(getClass());
    }

    /**
     * Starts startup stopwatch.
     */
    protected void startStopwatch() {
        startTstamp = U.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public final String getStartTimestampFormatted() {
        return DateFormat.getDateTimeInstance().format(new Date(startTstamp));
    }

    /** {@inheritDoc} */
    @Override public final String getUpTimeFormatted() {
        return X.timeSpan2HMSM(getUpTime());
    }

    /** {@inheritDoc} */
    @Override public final long getStartTimestamp() {
        return startTstamp;
    }

    /** {@inheritDoc} */
    @Override public final long getUpTime() {
        return startTstamp == 0 ? 0 : U.currentTimeMillis() - startTstamp;
    }

    /** {@inheritDoc} */
    @Override public UUID getLocalNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public final String getGridGainHome() {
        return ggHome;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /**
     * Sets SPI name.
     *
     * @param name SPI name.
     */
    @GridSpiConfiguration(optional = true)
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public final void onContextInitialized(final GridSpiContext spiCtx) throws GridSpiException {
        assert spiCtx != null;

        this.spiCtx = spiCtx;

        // Always run consistency check for security SPIs.
        final boolean secSpi = GridAuthenticationSpi.class.isAssignableFrom(getClass()) ||
            GridSecureSessionSpi.class.isAssignableFrom(getClass());

        final boolean check = secSpi || !Boolean.getBoolean(GG_SKIP_CONFIGURATION_CONSISTENCY_CHECK);

        if (check) {
            spiCtx.addLocalEventListener(paramsLsnr = new GridLocalEventListener() {
                @Override public void onEvent(IgniteEvent evt) {
                    assert evt instanceof IgniteDiscoveryEvent : "Invalid event [expected=" + EVT_NODE_JOINED +
                        ", actual=" + evt.type() + ", evt=" + evt + ']';

                    ClusterNode node = spiCtx.node(((IgniteDiscoveryEvent)evt).eventNode().id());

                    if (node != null)
                        try {
                            checkConfigurationConsistency(spiCtx, node, false, !secSpi);
                            checkConfigurationConsistency0(spiCtx, node, false);
                        }
                        catch (GridSpiException e) {
                            U.error(log, "Spi consistency check failed [node=" + node.id() + ", spi=" + getName() + ']',
                                e);
                        }
                }
            }, EVT_NODE_JOINED);

            final Collection<ClusterNode> remotes = F.concat(false, spiCtx.remoteNodes(), spiCtx.remoteDaemonNodes());

            for (ClusterNode node : remotes) {
                checkConfigurationConsistency(spiCtx, node, true, !secSpi);
                checkConfigurationConsistency0(spiCtx, node, true);
            }
        }

        onContextInitialized0(spiCtx);
    }

    /**
     * Method to be called in the end of onContextInitialized method.
     *
     * @param spiCtx SPI context.
     * @throws GridSpiException In case of errors.
     */
    protected void onContextInitialized0(final GridSpiContext spiCtx) throws GridSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final void onContextDestroyed() {
        onContextDestroyed0();

        if (spiCtx != null && paramsLsnr != null)
            spiCtx.removeLocalEventListener(paramsLsnr);

        ClusterNode locNode = spiCtx == null ? null : spiCtx.localNode();

        // Set dummy no-op context.
        spiCtx = new GridDummySpiContext(locNode);
    }

    /**
     * Method to be called in the beginning of onContextDestroyed() method.
     */
    protected void onContextDestroyed0() {
        // No-op.
    }

    /**
     * This method returns SPI internal instances that need to be injected as well.
     * Usually these will be instances provided to SPI externally by user, e.g. during
     * SPI configuration.
     *
     * @return Internal SPI objects that also need to be injected.
     */
    public Collection<Object> injectables() {
        return Collections.emptyList();
    }

    /**
     * Gets SPI context.
     *
     * @return SPI context.
     */
    public GridSpiContext getSpiContext() {
        return spiCtx;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws GridSpiException {
        return Collections.emptyMap();
    }

    /**
     * Throws exception with uniform error message if given parameter's assertion condition
     * is {@code false}.
     *
     * @param cond Assertion condition to check.
     * @param condDesc Description of failed condition. Note that this description should include
     *      JavaBean name of the property (<b>not</b> a variable name) as well condition in
     *      Java syntax like, for example:
     *      <pre name="code" class="java">
     *      ...
     *      assertParameter(dirPath != null, "dirPath != null");
     *      ...
     *      </pre>
     *      Note that in case when variable name is the same as JavaBean property you
     *      can just copy Java condition expression into description as a string.
     * @throws GridSpiException Thrown if given condition is {@code false}
     */
    protected final void assertParameter(boolean cond, String condDesc) throws GridSpiException {
        if (!cond)
            throw new GridSpiException("SPI parameter failed condition check: " + condDesc);
    }

    /**
     * Gets uniformly formatted message for SPI start.
     *
     * @return Uniformly formatted message for SPI start.
     */
    protected final String startInfo() {
        return "SPI started ok [startMs=" + getUpTime() + ", spiMBean=" + spiMBean + ']';
    }

    /**
     * Gets uniformly format message for SPI stop.
     *
     * @return Uniformly format message for SPI stop.
     */
    protected final String stopInfo() {
        return "SPI stopped ok.";
    }

    /**
     * Gets uniformed string for configuration parameter.
     *
     * @param name Parameter name.
     * @param val Parameter value.
     * @return Uniformed string for configuration parameter.
     */
    protected final String configInfo(String name, Object val) {
        assert name != null;

        return "Using parameter [" + name + '=' + val + ']';
    }

    /**
     * @param msg Error message.
     * @param locVal Local node value.
     * @return Error text.
     */
    private static String format(String msg, Object locVal) {
        return msg + U.nl() +
            ">>> => Local node:  " + locVal + U.nl();
    }

    /**
     * @param msg Error message.
     * @param locVal Local node value.
     * @param rmtVal Remote node value.
     * @return Error text.
     */
    private static String format(String msg, Object locVal, Object rmtVal) {
        return msg + U.nl() +
            ">>> => Local node:  " + locVal + U.nl() +
            ">>> => Remote node: " + rmtVal + U.nl();
    }

    /**
     * Registers SPI MBean. Note that SPI can only register one MBean.
     *
     * @param gridName Grid name. If null, then name will be empty.
     * @param impl MBean implementation.
     * @param mbeanItf MBean interface (if {@code null}, then standard JMX
     *    naming conventions are used.
     * @param <T> Type of the MBean
     * @throws GridSpiException If registration failed.
     */
    protected final <T extends GridSpiManagementMBean> void registerMBean(String gridName, T impl, Class<T> mbeanItf)
        throws GridSpiException {
        assert mbeanItf == null || mbeanItf.isInterface();
        assert jmx != null;

        try {
            spiMBean = U.registerMBean(jmx, gridName, "SPIs", getName(), impl, mbeanItf);

            if (log.isDebugEnabled())
                log.debug("Registered SPI MBean: " + spiMBean);
        }
        catch (JMException e) {
            throw new GridSpiException("Failed to register SPI MBean: " + spiMBean, e);
        }
    }

    /**
     * Unregisters MBean.
     *
     * @throws GridSpiException If bean could not be unregistered.
     */
    protected final void unregisterMBean() throws GridSpiException {
        // Unregister SPI MBean.
        if (spiMBean != null) {
            assert jmx != null;

            try {
                jmx.unregisterMBean(spiMBean);

                if (log.isDebugEnabled())
                    log.debug("Unregistered SPI MBean: " + spiMBean);
            }
            catch (JMException e) {
                throw new GridSpiException("Failed to unregister SPI MBean: " + spiMBean, e);
            }
        }
    }

    /**
     * @return {@code true} if this check is optional.
     */
    private boolean checkOptional() {
        GridSpiConsistencyChecked ann = U.getAnnotation(getClass(), GridSpiConsistencyChecked.class);

        return ann != null && ann.optional();
    }

    /**
     * @return {@code true} if this check is optional.
     */
    private boolean checkDaemon() {
        GridSpiConsistencyChecked ann = U.getAnnotation(getClass(), GridSpiConsistencyChecked.class);

        return ann != null && ann.checkDaemon();
    }

    /**
     * @return {@code true} if this check is enabled.
     */
    private boolean checkEnabled() {
        return U.getAnnotation(getClass(), GridSpiConsistencyChecked.class) != null;
    }

    /**
     * Method which is called in the end of checkConfigurationConsistency() method. May be overriden in SPIs.
     *
     * @param spiCtx SPI context.
     * @param node Remote node.
     * @param starting If this node is starting or not.
     * @throws GridSpiException in case of errors.
     */
    protected void checkConfigurationConsistency0(GridSpiContext spiCtx, ClusterNode node, boolean starting)
        throws GridSpiException {
        // No-op.
    }

    /**
     * Checks remote node SPI configuration and prints warnings if necessary.
     *
     * @param spiCtx SPI context.
     * @param node Remote node.
     * @param starting Flag indicating whether this method is called during SPI start or not.
     * @throws GridSpiException If check fatally failed.
     */
    @SuppressWarnings("IfMayBeConditional")
    private void checkConfigurationConsistency(GridSpiContext spiCtx, ClusterNode node, boolean starting, boolean tip)
        throws GridSpiException {
        assert spiCtx != null;
        assert node != null;

        if (node.isDaemon() && !checkDaemon()) {
            if (log.isDebugEnabled())
                log.debug("Skipping configuration consistency check for daemon node: " + node);

            return;
        }

        /*
         * Optional SPI means that we should not print warning if SPIs are different but
         * still need to compare attributes if SPIs are the same.
         */
        boolean optional = checkOptional();
        boolean enabled = checkEnabled();

        if (!enabled)
            return;

        String clsAttr = createSpiAttributeName(GridNodeAttributes.ATTR_SPI_CLASS);

        String name = getName();

        SB sb = new SB();

        /*
         * If there are any attributes do compare class and version
         * (do not print warning for the optional SPIs).
         */

        /* Check SPI class and version. */
        String locCls = spiCtx.localNode().attribute(clsAttr);
        String rmtCls = node.attribute(clsAttr);

        assert locCls != null : "Local SPI class name attribute not found: " + clsAttr;

        boolean isSpiConsistent = false;

        String tipStr = tip ? " (fix configuration or set " +
            "-D" + GG_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system property)" : "";

        if (rmtCls == null) {
            if (!optional && starting)
                throw new GridSpiException("Remote SPI with the same name is not configured" + tipStr + " [name=" + name +
                    ", loc=" + locCls + ']');

            sb.a(format(">>> Remote SPI with the same name is not configured: " + name, locCls));
        }
        else if (!locCls.equals(rmtCls)) {
            if (!optional && starting)
                throw new GridSpiException("Remote SPI with the same name is of different type" + tipStr + " [name=" + name +
                    ", loc=" + locCls + ", rmt=" + rmtCls + ']');

            sb.a(format(">>> Remote SPI with the same name is of different type: " + name, locCls, rmtCls));
        }
        else
            isSpiConsistent = true;

        if (optional && !isSpiConsistent)
            return;

        // It makes no sense to compare inconsistent SPIs attributes.
        if (isSpiConsistent) {
            List<String> attrs = getConsistentAttributeNames();

            // Process all SPI specific attributes.
            for (String attr : attrs) {
                // Ignore class and version attributes processed above.
                if (!attr.equals(clsAttr)) {
                    // This check is considered as optional if no attributes
                    Object rmtVal = node.attribute(attr);
                    Object locVal = spiCtx.localNode().attribute(attr);

                    if (locVal == null && rmtVal == null)
                        continue;

                    if (locVal == null || rmtVal == null || !locVal.equals(rmtVal))
                        sb.a(format(">>> Remote node has different " + getName() + " SPI attribute " +
                            attr, locVal, rmtVal));
                }
            }
        }

        if (sb.length() > 0) {
            String msg;

            if (starting)
                msg = U.nl() + U.nl() +
                    ">>> +--------------------------------------------------------------------+" + U.nl() +
                    ">>> + Courtesy notice that starting node has inconsistent configuration. +" + U.nl() +
                    ">>> + Ignore this message if you are sure that this is done on purpose.  +" + U.nl() +
                    ">>> +--------------------------------------------------------------------+" + U.nl() +
                    ">>> Remote Node ID: " + node.id().toString().toUpperCase() + U.nl() + sb;
            else
                msg = U.nl() + U.nl() +
                    ">>> +-------------------------------------------------------------------+" + U.nl() +
                    ">>> + Courtesy notice that joining node has inconsistent configuration. +" + U.nl() +
                    ">>> + Ignore this message if you are sure that this is done on purpose. +" + U.nl() +
                    ">>> +-------------------------------------------------------------------+" + U.nl() +
                    ">>> Remote Node ID: " + node.id().toString().toUpperCase() + U.nl() + sb;

            U.courtesy(log, msg);
        }
    }

    /**
     * Returns back a list of attributes that should be consistent
     * for this SPI. Consistency means that remote node has to
     * have the same attribute with the same value.
     *
     * @return List or attribute names.
     */
    protected List<String> getConsistentAttributeNames() {
        return Collections.emptyList();
    }

    /**
     * Creates new name for the given attribute. Name contains
     * SPI name prefix.
     *
     * @param attrName SPI attribute name.
     * @return New name with SPI name prefix.
     */
    protected String createSpiAttributeName(String attrName) {
        return U.spiAttribute(this, attrName);
    }

    /**
     * Temporarily SPI context.
     */
    private static class GridDummySpiContext implements GridSpiContext {
        /** */
        private final ClusterNode locNode;

        /**
         * Create temp SPI context.
         *
         * @param locNode Local node.
         */
        GridDummySpiContext(ClusterNode locNode) {
            this.locNode = locNode;
        }

        /** {@inheritDoc} */
        @Override public void addLocalEventListener(GridLocalEventListener lsnr, int... types) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void addMessageListener(GridMessageListener lsnr, String topic) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void recordEvent(IgniteEvent evt) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void registerPort(int port, GridPortProtocol proto) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void deregisterPort(int port, GridPortProtocol proto) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void deregisterPorts() {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public <K, V> V get(String cacheName, K key) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> V put(String cacheName, K key, V val, long ttl) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> V putIfAbsent(String cacheName, K key, V val, long ttl) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> V remove(String cacheName, K key) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K> boolean containsKey(String cacheName, K key) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeToSwap(String spaceName, Object key, @Nullable Object val,
            @Nullable ClassLoader ldr) throws GridException {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public <T> T readFromSwap(String spaceName, GridSwapKey key, @Nullable ClassLoader ldr)
            throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T readFromOffheap(String spaceName, int part, Object key, byte[] keyBytes,
            @Nullable ClassLoader ldr) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean removeFromOffheap(@Nullable String spaceName, int part, Object key,
            @Nullable byte[] keyBytes) throws GridException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeToOffheap(@Nullable String spaceName, int part, Object key,
            @Nullable byte[] keyBytes, Object val, @Nullable byte[] valBytes, @Nullable ClassLoader ldr)
            throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partition(String cacheName, Object key) {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public void removeFromSwap(String spaceName, Object key, @Nullable ClassLoader ldr)
            throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> nodes() {
            return  locNode == null  ? Collections.<ClusterNode>emptyList() : Collections.singletonList(locNode);
        }

        /** {@inheritDoc} */
        @Override public ClusterNode localNode() {
            return locNode;
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> remoteDaemonNodes() {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Nullable @Override
        public ClusterNode node(UUID nodeId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> remoteNodes() {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public boolean pingNode(UUID nodeId) {
            return locNode != null && nodeId.equals(locNode.id());
        }

        /** {@inheritDoc} */
        @Override public boolean removeLocalEventListener(GridLocalEventListener lsnr) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isEventRecordable(int... types) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean removeMessageListener(GridMessageListener lsnr, String topic) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void send(ClusterNode node, Serializable msg, String topic) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridNodeValidationResult validateNode(ClusterNode node) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean writeDelta(UUID nodeId, Object msg, ByteBuffer buf) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean readDelta(UUID nodeId, Class<?> msgCls, ByteBuffer buf) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridSecuritySubject> authenticatedSubjects() throws GridException {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public GridSecuritySubject authenticatedSubject(UUID subjId) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T readValueFromOffheapAndSwap(@Nullable String spaceName, Object key,
            @Nullable ClassLoader ldr) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridTcpMessageFactory messageFactory() {
            return null;
        }
    }
}
