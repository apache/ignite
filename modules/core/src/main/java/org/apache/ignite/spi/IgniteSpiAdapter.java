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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.resources.*;

import org.jetbrains.annotations.*;

import javax.management.*;
import java.io.*;
import java.text.*;
import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.events.EventType.*;

/**
 * This class provides convenient adapter for SPI implementations.
 */
public abstract class IgniteSpiAdapter implements IgniteSpi, IgniteSpiManagementMBean {
    /** */
    private ObjectName spiMBean;

    /** SPI start timestamp. */
    private long startTstamp;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    protected Ignite ignite;

    /** Grid instance name. */
    protected String gridName;

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
     *  Failure detection timeout. Initialized with the value of
     *  {@link IgniteConfiguration#getFailureDetectionTimeout()}.
     */
    private long failureDetectionTimeout;

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
        return ignite.cluster().localNode().id();
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
    @Override public final String getIgniteHome() {
        return ignite.configuration().getIgniteHome();
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
    @IgniteSpiConfiguration(optional = true)
    public void setName(String name) {
        this.name = name;
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

            final Collection<ClusterNode> remotes = F.concat(false, spiCtx.remoteNodes(), spiCtx.remoteDaemonNodes());

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

        if (ignite != null)
            gridName = ignite.name();
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
     * @throws IgniteSpiException If registration failed.
     */
    protected final <T extends IgniteSpiManagementMBean> void registerMBean(String gridName, T impl, Class<T> mbeanItf)
        throws IgniteSpiException {
        MBeanServer jmx = ignite.configuration().getMBeanServer();

        assert mbeanItf == null || mbeanItf.isInterface();
        assert jmx != null;

        try {
            spiMBean = U.registerMBean(jmx, gridName, "SPIs", getName(), impl, mbeanItf);

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
        if (spiMBean != null) {
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

        if (!enabled)
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
                    " [name=" + name + ", loc=" + locCls + ']');

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
        }
        // Intentionally compare references using '!=' below
        else if (ignite.configuration().getFailureDetectionTimeout() !=
                IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT)
            log.warning("Failure detection timeout will be ignored (one of SPI parameters has been set explicitly)");

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
                    @Nullable @Override public Message create(byte type) {
                        throw new IgniteException("Failed to read message, node is not started.");
                    }
                };
            }

            if (msgFormatter0 == null) {
                msgFormatter0 = new MessageFormatter() {
                    @Override public MessageWriter writer() {
                        throw new IgniteException("Failed to write message, node is not started.");
                    }

                    @Override public MessageReader reader(MessageFactory factory, Class<? extends Message> msgCls) {
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

            ((IgniteKernal)ignite0).context().timeout().addTimeoutObject(new GridSpiTimeoutObject(obj));
        }

        /** {@inheritDoc} */
        @Override public void removeTimeoutObject(IgniteSpiTimeoutObject obj) {
            Ignite ignite0 = ignite;

            if (!(ignite0 instanceof IgniteKernal))
                throw new IgniteSpiException("Wrong Ignite instance is set: " + ignite0);

            ((IgniteKernal)ignite0).context().timeout().removeTimeoutObject(new GridSpiTimeoutObject(obj));
        }
    }
}
