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

package org.apache.ignite.internal.processors.streamer;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.streamer.*;
import org.apache.ignite.streamer.index.*;
import org.apache.ignite.streamer.window.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.internal.IgniteNodeAttributes.*;

/**
 *
 */
public class GridStreamProcessor extends GridProcessorAdapter {
    /** Streamers map. */
    private Map<String, IgniteStreamerImpl> map;

    /** Registered MBeans */
    private Collection<ObjectName> mBeans;

    /** MBean server. */
    private final MBeanServer mBeanSrv;

    /**
     * @param ctx Kernal context.
     */
    public GridStreamProcessor(GridKernalContext ctx) {
        super(ctx);

        mBeanSrv = ctx.config().getMBeanServer();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        super.onKernalStart();

        if (!getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
            for (ClusterNode n : ctx.discovery().remoteNodes())
                checkStreamer(n);
        }

        for (IgniteStreamerImpl s : map.values()) {
            try {
                mBeans.add(U.registerMBean(mBeanSrv, ctx.gridName(), U.maskName(s.name()), "Streamer",
                    new StreamerMBeanAdapter(s), StreamerMBean.class));

                if (log.isDebugEnabled())
                    log.debug("Registered MBean for streamer: " + s.name());
            }
            catch (JMException e) {
                U.error(log, "Failed to register streamer MBean: " + s.name(), e);
            }

            // Add mbeans for stages.
            for (StreamerStage stage : s.configuration().getStages()) {
                try {
                    mBeans.add(U.registerMBean(mBeanSrv, ctx.gridName(), U.maskName(s.name()), "Stage-" + stage.name(),
                        new StreamerStageMBeanAdapter(stage.name(), stage.getClass().getName(), s),
                        StreamerStageMBean.class));

                    if (log.isDebugEnabled())
                        log.debug("Registered MBean for streamer stage [streamer=" + s.name() +
                            ", stage=" + stage.name() + ']');
                }
                catch (JMException e) {
                    U.error(log, "Failed to register streamer stage MBean [streamer=" + s.name() +
                        ", stage=" + stage.name() + ']', e);
                }
            }

            // Add mbeans for windows.
            for (StreamerWindow win : s.configuration().getWindows()) {
                try {
                    if (hasInterface(win.getClass(), StreamerWindowMBean.class)) {
                        mBeans.add(U.registerMBean(mBeanSrv, ctx.gridName(), U.maskName(s.name()),
                            "Window-" + win.name(),
                            (StreamerWindowMBean)win,
                            StreamerWindowMBean.class));

                        if (log.isDebugEnabled())
                            log.debug("Registered MBean for streamer window [streamer=" + s.name() +
                                ", window=" + win.name() + ']');
                    }
                }
                catch (JMException e) {
                    U.error(log, "Failed to register streamer window MBean [streamer=" + s.name() +
                        ", window=" + win.name() + ']', e);
                }

                if (win instanceof StreamerWindowAdapter) {
                    StreamerIndexProvider[] idxs = ((StreamerWindowAdapter)win).indexProviders();

                    if (idxs != null && idxs.length > 0) {
                        for (StreamerIndexProvider idx : idxs) {
                            try {
                                mBeans.add(U.registerMBean(mBeanSrv, ctx.gridName(), U.maskName(s.name()),
                                    "Window-" + win.name() + "-index-" + idx.name(), idx,
                                    StreamerIndexProviderMBean.class));

                                if (log.isDebugEnabled())
                                    log.debug("Registered MBean for streamer window index [streamer=" + s.name() +
                                        ", window=" + win.name() + ", index=" + idx.name() + ']');
                            }
                            catch (JMException e) {
                                U.error(log, "Failed to register streamer index MBean [streamer=" + s.name() +
                                    ", window=" + win.name() + ", index=" + idx.name() + ']', e);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Check configuration identity on local and remote nodes.
     *
     * @param rmtNode Remote node to check.
     * @throws IgniteCheckedException If configuration mismatch detected.
     */
    private void checkStreamer(ClusterNode rmtNode) throws IgniteCheckedException {
        GridStreamerAttributes[] rmtAttrs = rmtNode.attribute(ATTR_STREAMER);
        GridStreamerAttributes[] locAttrs = ctx.discovery().localNode().attribute(ATTR_STREAMER);

        // If local or remote streamer is not configured, nothing to validate.
        if (F.isEmpty(locAttrs) || F.isEmpty(rmtAttrs))
            return;

        for (GridStreamerAttributes rmtAttr : rmtAttrs) {
            for (GridStreamerAttributes locAttr : locAttrs) {
                if (!F.eq(rmtAttr.name(), locAttr.name()))
                    continue;

                if (rmtAttr.atLeastOnce() != locAttr.atLeastOnce())
                    throw new IgniteCheckedException("Streamer atLeastOnce configuration flag mismatch (fix atLeastOnce flag " +
                        "in streamer configuration or set " +
                        "-D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system " +
                        "property) [streamer=" + locAttr.name() +
                        ", locAtLeastOnce=" + locAttr.atLeastOnce() +
                        ", rmtAtLeastOnce=" + rmtAttr.atLeastOnce() +
                        ", rmtNodeId=" + rmtNode.id() + ']');

                if (!rmtAttr.stages().equals(locAttr.stages()))
                    throw new IgniteCheckedException("Streamer stages configuration mismatch (fix streamer stages " +
                        "configuration or set " +
                        "-D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system " +
                        "property) [streamer=" + locAttr.name() +
                        ", locStages=" + locAttr.stages() +
                        ", rmtStages=" + rmtAttr.stages() +
                        ", rmtNodeId=" + rmtNode.id() + ']');

                if (rmtAttr.atLeastOnce()) {
                    if (rmtAttr.maxFailoverAttempts() != locAttr.maxFailoverAttempts())
                        U.warn(log, "Streamer maxFailoverAttempts configuration property differs on local and remote " +
                            "nodes (ignore this message if it is done on purpose) [streamer=" + locAttr.name() +
                            ", locMaxFailoverAttempts=" + locAttr.maxFailoverAttempts() +
                            ", rmtMaxFailoverAttempts=" + rmtAttr.maxFailoverAttempts() +
                            ", rmtNodeId=" + rmtNode.id() + ']');

                    if (rmtAttr.maxConcurrentSessions() != locAttr.maxConcurrentSessions())
                        U.warn(log, "Streamer maxConcurrentSessions configuration property differs on local and " +
                            "remote nodes (ignore this message if it is done on purpose) [streamer=" + locAttr.name() +
                            ", locMaxConcurrentSessions=" + locAttr.maxConcurrentSessions() +
                            ", rmtMaxConcurrentSessions=" + rmtAttr.maxConcurrentSessions() +
                            ", rmtNodeId=" + rmtNode.id() + ']');
                }
            }
        }
    }

    /**
     * Traverses class hierarchy and checks if class implements given interface.
     *
     * @param cls Class to check.
     * @param iface Interface to search for.
     * @return {@code True} if at least one parent implements given interface.
     */
    private boolean hasInterface(Class<?> cls, Class<?> iface) {
        while (cls != null) {
            Class<?>[] interfaces = cls.getInterfaces();

            for (Class<?> iface0 : interfaces) {
                if (iface0.equals(iface))
                    return true;
            }

            cls = cls.getSuperclass();
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        super.start();

        StreamerConfiguration[] cfg = ctx.config().getStreamerConfiguration();

        if (F.isEmpty(cfg)) {
            map = Collections.emptyMap();

            return;
        }
        else {
            int len = cfg.length;

            map = new HashMap<>(len, 1.0f);

            mBeans = new ArrayList<>(len);
        }

        for (StreamerConfiguration c : cfg) {
            IgniteStreamerImpl s = new IgniteStreamerImpl(ctx, c);

            s.start();

            IgniteStreamerImpl old = map.put(c.getName(), s);

            if (old != null) {
                old.stop(true);

                throw new IgniteCheckedException("Duplicate streamer name found (check configuration and " +
                    "assign unique name to each streamer): " + c.getName());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        super.onKernalStop(cancel);

        if (!F.isEmpty(mBeans)) {
            for (ObjectName name : mBeans) {
                try {
                    mBeanSrv.unregisterMBean(name);
                }
                catch (JMException e) {
                    U.error(log, "Failed to unregister streamer MBean.", e);
                }
            }
        }

        for (IgniteStreamerImpl streamer : map.values())
            streamer.onKernalStop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        super.stop(cancel);

        for (IgniteStreamerImpl s : map.values())
            s.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void addAttributes(Map<String, Object> attrs) throws IgniteCheckedException {
        super.addAttributes(attrs);

        StreamerConfiguration[] cfg = ctx.config().getStreamerConfiguration();

        if (F.isEmpty(cfg))
            return;

        GridStreamerAttributes[] arr = new GridStreamerAttributes[cfg.length];

        int i = 0;

        for (StreamerConfiguration c : cfg)
            arr[i++] = new GridStreamerAttributes(c);

        attrs.put(ATTR_STREAMER, arr);
    }

    /**
     * @return Default no-name streamer.
     */
    public IgniteStreamer streamer() {
        return streamer(null);
    }

    /**
     * @param name Streamer name.
     * @return Streamer for given name.
     */
    public IgniteStreamer streamer(@Nullable String name) {
        IgniteStreamer streamer = map.get(name);

        if (streamer == null)
            throw new IllegalArgumentException("Streamer is not configured: " + name);

        return streamer;
    }

    /**
     * @return All configured streamers.
     */
    public Collection<IgniteStreamer> streamers() {
        Collection<IgniteStreamerImpl> streamers = map.values();

        Collection<IgniteStreamer> res = new ArrayList<>(streamers.size());

        streamers.addAll(map.values());

        return res;
    }

    /**
     * Callback for undeployed class loaders.
     *
     * @param ldr Class loader.
     */
    public void onUndeployed(ClassLoader ldr) {
        for (IgniteStreamerEx streamer : map.values())
            streamer.onUndeploy(ldr);
    }
}
