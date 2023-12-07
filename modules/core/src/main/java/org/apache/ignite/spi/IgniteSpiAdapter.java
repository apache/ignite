/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.timeout.GridSpiTimeoutObject;
import org.apache.ignite.internal.util.IgniteExceptionRegistry;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * This class provides convenient adapter for SPI implementations.
 */
public abstract class IgniteSpiAdapter implements IgniteSpi {
    /** */
    private ObjectName spiMBean;

    /** SPI start timestamp. */
    private long startTstamp;

    /** */
    @LoggerResource
    protected IgniteLogger log;

    /** Ignite instance. */
    protected Ignite ignite;

    /** Ignite instance name. */
    protected String igniteInstanceName;

    /** SPI name. */
    private String name;

    /** Grid SPI context. */
    private volatile IgniteSpiContext spiCtx = new GridDummySpiContext(null, false, null);

    /** Discovery listener. */
    private GridLocalEventListener paramsLsnr;

    /** Local node. */
    private ClusterNode locNode;

    /** Failure detection timeout usage switch. */
    private boolean failureDetectionTimeoutEnabled = true;

    /**
     * Failure detection timeout for client nodes. Initialized with the value of
     * {@link IgniteConfiguration#getClientFailureDetectionTimeout()}.
     */
    private long clientFailureDetectionTimeout;

    /**
     * Failure detection timeout. Initialized with the value of
     * {@link IgniteConfiguration#getFailureDetectionTimeout()}.
     */
    private long failureDetectionTimeout;

    /** Start flag to deny repeating start attempts. */
    private final AtomicBoolean startedFlag = new AtomicBoolean();

    /**
     * Creates new adapter and initializes it from the current (this) class.
     * SPI name will be initialized to the simple name of the class
     * (see {@link Class#getSimpleName()}).
     */
    protected IgniteSpiAdapter() {
        name = U.getSimpleName(getClass());
    }

    /**
     * Starts startup stopwatch.
     */
    protected void startStopwatch() {
        startTstamp = U.currentTimeMillis();
    }

    /**
     * This method is called by built-in managers implementation to avoid
     * repeating SPI start attempts.
     */
    public final void onBeforeStart() {
        if (!startedFlag.compareAndSet(false, true))
            throw new IllegalStateException("SPI has already been started " +
                "(always create new configuration instance for each starting Ignite instances) " +
                "[spi=" + this + ']');
    }

    /**
     * Checks if {@link #onBeforeStart()} has been called on this SPI instance.
     *
     * @return {@code True} if {@link #onBeforeStart()} has already been called.
     */
    public final boolean started() {
        return startedFlag.get();
    }

    /**
     * @return Local node.
     */
    protected ClusterNode getLocalNode() {
        if (locNode != null)
            return locNode;

        locNode = getSpiContext().localNode();

        return locNode;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /**
     * Gets ignite instance.
     *
     * @return Ignite instance.
     */
    public Ignite ignite() {
        return ignite;
    }

    /**
     * Sets SPI name.
     *
     * @param name SPI name.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public IgniteSpiAdapter setName(String name) {
        this.name = name;

        return this;
    }

    /** {@inheritDoc} */
    @Override public final void onContextInitialized(final IgniteSpiContext spiCtx) throws IgniteSpiException {
        assert spiCtx != null;

        this.spiCtx = spiCtx;

        if (!Boolean.getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
            spiCtx.addLocalEventListener(paramsLsnr = new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    assert evt instanceof DiscoveryEvent : "Invalid event [expected=" + EVT_NODE_JOINED +
                        ", actual=" + evt.type() + ", evt=" + evt + ']';

                    ClusterNode node = spiCtx.node(((DiscoveryEvent)evt).eventNode().id());

                    if (node != null)
                        try {
                            checkConfigurationConsistency(spiCtx, node, false);
                            checkConfigurationConsistency0(spiCtx, node, false);
                        }
                        catch (IgniteSpiException e) {
                            U.error(log, "Spi consistency check failed [node=" + node.id() + ", spi=" + getName() + ']',
                                e);
                        }
                }
            }, EVT_NODE_JOINED);

            final Collection<ClusterNode> remotes = spiCtx.remoteNodes();

            for (ClusterNode node : remotes) {
                checkConfigurationConsistency(spiCtx, node, true);
                checkConfigurationConsistency0(spiCtx, node, true);
            }
        }

        onContextInitialized0(spiCtx);
    }

    /**
     * Method to be called in the end of onContextInitialized method.
     *
     * @param spiCtx SPI context.
     * @throws IgniteSpiException In case of errors.
     */
    protected void onContextInitialized0(final IgniteSpiContext spiCtx) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final void onContextDestroyed() {
        onContextDestroyed0();

        if (spiCtx != null && paramsLsnr != null)
            spiCtx.removeLocalEventListener(paramsLsnr);

        ClusterNode locNode = spiCtx == null ? null : spiCtx.localNode();

        // Set dummy no-op context.
        spiCtx = new GridDummySpiContext(locNode, true, spiCtx);
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnected(IgniteFuture<?> reconnectFut) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onClientReconnected(boolean clusterRestarted) {
        // No-op.
    }

    /**
     * Inject ignite instance.
     *
     * @param ignite Ignite instance.
     */
    @IgniteInstanceResource
    protected void injectResources(Ignite ignite) {
        this.ignite = ignite;

        if (ignite != null && igniteInstanceName == null)
            igniteInstanceName = ignite.name();
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
    public IgniteSpiContext getSpiContext() {
        return spiCtx;
    }

    /**
     * Gets Exception registry.
     *
     * @return Exception registry.
     */
    public IgniteExceptionRegistry getExceptionRegistry() {
        return IgniteExceptionRegistry.get();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
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
     * @throws IgniteSpiException Thrown if given condition is {@code false}
     */
    protected final void assertParameter(boolean cond, String condDesc) throws IgniteSpiException {
        if (!cond)
            throw new IgniteSpiException("SPI parameter failed condition check: " + condDesc);
    }

    /**
     * Gets uniformly formatted message for SPI start.
     *
     * @return Uniformly formatted message for SPI start.
     */
    protected final String startInfo() {
        return "SPI started ok [startMs=" + startTstamp + ", spiMBean=" + spiMBean + ']';
    }

    /**
     * Gets SPI startup time.
     * @return Time in millis.
     */
    final long getStartTstamp() {
        return startTstamp;
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
     * @param igniteInstanceName Ignite instance name. If null, then name will be empty.
     * @param impl MBean implementation.
     * @param mbeanItf MBean interface (if {@code null}, then standard JMX
     *    naming conventions are used.
     * @param <T> Type of the MBean
     * @throws IgniteSpiException If registration failed.
     */
    protected final <T extends IgniteSpiManagementMBean> void registerMBean(
        String igniteInstanceName,
        T impl,
        Class<T> mbeanItf
    ) throws IgniteSpiException {
        if (ignite == null || U.IGNITE_MBEANS_DISABLED)
            return;

        MBeanServer jmx = ignite.configuration().getMBeanServer();

        assert mbeanItf == null || mbeanItf.isInterface();
        assert jmx != null;

        try {
            spiMBean = U.registerMBean(jmx, igniteInstanceName, "SPIs", getName(), impl, mbeanItf);

            if (log.isDebugEnabled())
                log.debug("Registered SPI MBean: " + spiMBean);
        }
        catch (JMException e) {
            throw new IgniteSpiException("Failed to register SPI MBean: " + spiMBean, e);
        }
    }

    /**
     * Unregisters MBean.
     *
     * @throws IgniteSpiException If bean could not be unregistered.
     */
    protected final void unregisterMBean() throws IgniteSpiException {
        // Unregister SPI MBean.
        if (spiMBean != null && ignite != null) {
            assert !U.IGNITE_MBEANS_DISABLED;

            MBeanServer jmx = ignite.configuration().getMBeanServer();

            assert jmx != null;

            try {
                jmx.unregisterMBean(spiMBean);

                if (log.isDebugEnabled())
                    log.debug("Unregistered SPI MBean: " + spiMBean);
            }
            catch (JMException e) {
                throw new IgniteSpiException("Failed to unregister SPI MBean: " + spiMBean, e);
            }
        }
    }

    /**
     * @return {@code True} if node is stopping.
     */
    protected final boolean isNodeStopping() {
        return spiCtx.isStopping();
    }

    /**
     * @return {@code true} if this check is optional.
     */
    private boolean checkOptional() {
        IgniteSpiConsistencyChecked ann = U.getAnnotation(getClass(), IgniteSpiConsistencyChecked.class);

        return ann != null && ann.optional();
    }

    /**
     * @return {@code true} if this check is enabled.
     */
    private boolean checkEnabled() {
        return U.getAnnotation(getClass(), IgniteSpiConsistencyChecked.class) != null;
    }

    /**
     * @return {@code true} if client cluster nodes should be checked.
     */
    private boolean checkClient() {
        IgniteSpiConsistencyChecked ann = U.getAnnotation(getClass(), IgniteSpiConsistencyChecked.class);

        return ann != null && ann.checkClient();
    }

    /**
     * Method which is called in the end of checkConfigurationConsistency() method. May be overriden in SPIs.
     *
     * @param spiCtx SPI context.
     * @param node Remote node.
     * @param starting If this node is starting or not.
     * @throws IgniteSpiException in case of errors.
     */
    protected void checkConfigurationConsistency0(IgniteSpiContext spiCtx, ClusterNode node, boolean starting)
        throws IgniteSpiException {
        // No-op.
    }

    /**
     * Checks remote node SPI configuration and prints warnings if necessary.
     *
     * @param spiCtx SPI context.
     * @param node Remote node.
     * @param starting Flag indicating whether this method is called during SPI start or not.
     * @throws IgniteSpiException If check fatally failed.
     */
    @SuppressWarnings("IfMayBeConditional")
    private void checkConfigurationConsistency(IgniteSpiContext spiCtx, ClusterNode node, boolean starting)
        throws IgniteSpiException {
        assert spiCtx != null;
        assert node != null;

        /*
         * Optional SPI means that we should not print warning if SPIs are different but
         * still need to compare attributes if SPIs are the same.
         */
        boolean optional = checkOptional();
        boolean enabled = checkEnabled();
        boolean checkClient = checkClient();

        if (!enabled)
            return;

        if (!checkClient && (getLocalNode().isClient() || node.isClient()))
            return;

        String clsAttr = createSpiAttributeName(IgniteNodeAttributes.ATTR_SPI_CLASS);

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

        String tipStr = " (fix configuration or set " +
            "-D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system property)";

        if (rmtCls == null) {
            if (!optional && starting)
                throw new IgniteSpiException("Remote SPI with the same name is not configured" + tipStr +
                    " [name=" + name + ", loc=" + locCls + ", locNode=" + spiCtx.localNode() + ", rmt=" + rmtCls +
                    ", rmtNode=" + node + ']');

            sb.a(format(">>> Remote SPI with the same name is not configured: " + name, locCls));
        }
        else if (!locCls.equals(rmtCls)) {
            if (!optional && starting)
                throw new IgniteSpiException("Remote SPI with the same name is of different type" + tipStr +
                    " [name=" + name + ", loc=" + locCls + ", rmt=" + rmtCls + ']');

            sb.a(format(">>> Remote SPI with the same name is of different type: " + name, locCls, rmtCls));
        }
        else
            isSpiConsistent = true;

        // It makes no sense to compare inconsistent SPIs attributes.
        if (!optional && isSpiConsistent) {
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
     * @param obj Timeout object.
     * @see IgniteSpiContext#addTimeoutObject(IgniteSpiTimeoutObject)
     */
    protected void addTimeoutObject(IgniteSpiTimeoutObject obj) {
        spiCtx.addTimeoutObject(obj);
    }

    /**
     * @param obj Timeout object.
     * @see IgniteSpiContext#removeTimeoutObject(IgniteSpiTimeoutObject)
     */
    protected void removeTimeoutObject(IgniteSpiTimeoutObject obj) {
        spiCtx.removeTimeoutObject(obj);
    }

    /**
     * Initiates and checks failure detection timeout value.
     */
    protected void initFailureDetectionTimeout() {
        if (failureDetectionTimeoutEnabled) {
            failureDetectionTimeout = ignite.configuration().getFailureDetectionTimeout();

            if (failureDetectionTimeout <= 0)
                throw new IgniteSpiException("Invalid failure detection timeout value: " + failureDetectionTimeout);
            else if (failureDetectionTimeout <= 10)
                // Because U.currentTimeInMillis() is updated once in 10 milliseconds.
                log.warning("Failure detection timeout is too low, it may lead to unpredictable behaviour " +
                    "[failureDetectionTimeout=" + failureDetectionTimeout + ']');
            else if (failureDetectionTimeout <= ignite.configuration().getMetricsUpdateFrequency())
                log.warning("'IgniteConfiguration.failureDetectionTimeout' should be greater then " +
                    "'IgniteConfiguration.metricsUpdateFrequency' to prevent unnecessary status checking.");
        }
        // Intentionally compare references using '!=' below
        else if (ignite.configuration().getFailureDetectionTimeout() != DFLT_FAILURE_DETECTION_TIMEOUT)
            log.warning("Failure detection timeout will be ignored (one of SPI parameters has been set explicitly)");

        clientFailureDetectionTimeout = ignite.configuration().getClientFailureDetectionTimeout();

        if (clientFailureDetectionTimeout <= 0)
            throw new IgniteSpiException("Invalid client failure detection timeout value: " +
                clientFailureDetectionTimeout);
        else if (clientFailureDetectionTimeout <= 10)
            // Because U.currentTimeInMillis() is updated once in 10 milliseconds.
            log.warning("Client failure detection timeout is too low, it may lead to unpredictable behaviour " +
                "[clientFailureDetectionTimeout=" + clientFailureDetectionTimeout + ']');

        if (clientFailureDetectionTimeout < ignite.configuration().getMetricsUpdateFrequency())
            throw new IgniteSpiException("Inconsistent configuration " +
                "('IgniteConfiguration.clientFailureDetectionTimeout' must be greater or equal to " +
                "'IgniteConfiguration.metricsUpdateFrequency').");
    }

    /**
     * Enables or disables failure detection timeout.
     *
     * @param enabled {@code true} if enable, {@code false} otherwise.
     */
    public void failureDetectionTimeoutEnabled(boolean enabled) {
        failureDetectionTimeoutEnabled = enabled;
    }

    /**
     * Checks whether failure detection timeout is enabled for this {@link IgniteSpi}.
     *
     * @return {@code true} if enabled, {@code false} otherwise.
     */
    public boolean failureDetectionTimeoutEnabled() {
        return failureDetectionTimeoutEnabled;
    }

    /**
     * Returns client failure detection timeout set to use for network related operations.
     *
     * @return client failure detection timeout in milliseconds or {@code 0} if the timeout is disabled.
     */
    public long clientFailureDetectionTimeout() {
        return clientFailureDetectionTimeout;
    }

    /**
     * Returns failure detection timeout set to use for network related operations.
     *
     * @return failure detection timeout in milliseconds or {@code 0} if the timeout is disabled.
     */
    public long failureDetectionTimeout() {
        return failureDetectionTimeout;
    }

    /**
     * Temporarily SPI context.
     */
    private class GridDummySpiContext implements IgniteSpiContext {
        /** */
        private final ClusterNode locNode;

        /** */
        private final boolean stopping;

        /** */
        private final MessageFactory msgFactory;

        /** */
        private final MessageFormatter msgFormatter;

        /**
         * Create temp SPI context.
         *
         * @param locNode Local node.
         * @param stopping Node stopping flag.
         * @param spiCtx SPI context.
         */
        GridDummySpiContext(ClusterNode locNode, boolean stopping, @Nullable IgniteSpiContext spiCtx) {
            this.locNode = locNode;
            this.stopping = stopping;

            MessageFactory msgFactory0 = spiCtx != null ? spiCtx.messageFactory() : null;
            MessageFormatter msgFormatter0 = spiCtx != null ? spiCtx.messageFormatter() : null;

            if (msgFactory0 == null) {
                msgFactory0 = new MessageFactory() {
                    @Nullable @Override public Message create(short type) {
                        throw new IgniteException("Failed to read message, node is not started.");
                    }
                };
            }

            if (msgFormatter0 == null) {
                msgFormatter0 = new MessageFormatter() {
                    @Override public MessageWriter writer(UUID rmtNodeId) {
                        throw new IgniteException("Failed to write message, node is not started.");
                    }

                    @Override public MessageReader reader(UUID rmtNodeId, MessageFactory msgFactory) {
                        throw new IgniteException("Failed to read message, node is not started.");
                    }
                };
            }

            this.msgFactory = msgFactory0;
            this.msgFormatter = msgFormatter0;
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
        @Override public void addLocalMessageListener(Object topic, IgniteBiPredicate<UUID, ?> p) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void recordEvent(Event evt) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void registerPort(int port, IgnitePortProtocol proto) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void deregisterPort(int port, IgnitePortProtocol proto) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void deregisterPorts() {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public <K, V> V get(String cacheName, K key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> V put(String cacheName, K key, V val, long ttl) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> V putIfAbsent(String cacheName, K key, V val, long ttl) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> V remove(String cacheName, K key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K> boolean containsKey(String cacheName, K key) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int partition(String cacheName, Object key) {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> nodes() {
            return locNode == null ? Collections.emptyList() : Collections.singletonList(locNode);
        }

        /** {@inheritDoc} */
        @Override public ClusterNode localNode() {
            return locNode;
        }

        /** {@inheritDoc} */
        @Nullable @Override public ClusterNode node(UUID nodeId) {
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
        @Override public void removeLocalMessageListener(Object topic, IgniteBiPredicate<UUID, ?> p) {
             /* No-op. */
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
        @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node, DiscoveryDataBag discoData) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<SecuritySubject> authenticatedSubjects() {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public SecuritySubject authenticatedSubject(UUID subjId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public MessageFormatter messageFormatter() {
            return msgFormatter;
        }

        /** {@inheritDoc} */
        @Override public MessageFactory messageFactory() {
            return msgFactory;
        }

        /** {@inheritDoc} */
        @Override public boolean isStopping() {
            return stopping;
        }

        /** {@inheritDoc} */
        @Override public boolean tryFailNode(UUID nodeId, @Nullable String warning) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void failNode(UUID nodeId, @Nullable String warning) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void addTimeoutObject(IgniteSpiTimeoutObject obj) {
            Ignite ignite0 = ignite;

            if (!(ignite0 instanceof IgniteKernal))
                throw new IgniteSpiException("Wrong Ignite instance is set: " + ignite0);

            ((IgniteEx)ignite0).context().timeout().addTimeoutObject(new GridSpiTimeoutObject(obj));
        }

        /** {@inheritDoc} */
        @Override public void removeTimeoutObject(IgniteSpiTimeoutObject obj) {
            Ignite ignite0 = ignite;

            if (!(ignite0 instanceof IgniteKernal))
                throw new IgniteSpiException("Wrong Ignite instance is set: " + ignite0);

            ((IgniteEx)ignite0).context().timeout().removeTimeoutObject(new GridSpiTimeoutObject(obj));
        }

        /** {@inheritDoc} */
        @Override public Map<String, Object> nodeAttributes() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public boolean communicationFailureResolveSupported() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void resolveCommunicationFailure(ClusterNode node, Exception err) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public ReadOnlyMetricRegistry getOrCreateMetricRegistry(String name) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void removeMetricRegistry(String name) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Iterable<ReadOnlyMetricRegistry> metricRegistries() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void addMetricRegistryCreationListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
            // No-op.
        }
    }
}
