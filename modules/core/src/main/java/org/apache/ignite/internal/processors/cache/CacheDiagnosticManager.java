/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import javax.management.InstanceNotFoundException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerMXBean;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Component for manage all cache diagnostic functionality.
 */
public class CacheDiagnosticManager extends GridCacheSharedManagerAdapter {
    /** Diagnostic mxbeans group name. */
    public static final String MBEAN_GROUP = "Diagnostic";

    /** Page lock tracker manager */
    private  PageLockTrackerManager pageLockTrackerManager;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        String name = cctx.kernalContext().pdsFolderResolver().resolveFolders().consistentId().toString();

        pageLockTrackerManager = new PageLockTrackerManager(log, name);

        pageLockTrackerManager.start();

        registerMetricsMBean(
            cctx.gridConfig(),
            MBEAN_GROUP,
            PageLockTrackerMXBean.MBEAN_NAME,
            pageLockTrackerManager.mxBean(),
            PageLockTrackerMXBean.class
        );
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        unregisterMetricsMBean(cctx.gridConfig(), MBEAN_GROUP, PageLockTrackerMXBean.MBEAN_NAME);

        pageLockTrackerManager.stop();
    }

    /**
     * Getter.
     *
     * @return Page lock tracker mananger.
     */
    public PageLockTrackerManager pageLockTracker() {
        return pageLockTrackerManager;
    }

    /**
     * @param cfg Ignite configuration.
     * @param groupName Name of group.
     * @param mbeanName Metrics MBean name.
     * @param impl Metrics implementation.
     * @param clazz Metrics class type.
     */
    protected <T> void registerMetricsMBean(
        IgniteConfiguration cfg,
        String groupName,
        String mbeanName,
        T impl,
        Class<T> clazz
    ) {
        if (U.IGNITE_MBEANS_DISABLED)
            return;

        try {
            U.registerMBean(
                cfg.getMBeanServer(),
                cfg.getIgniteInstanceName(),
                groupName,
                mbeanName,
                impl,
                clazz);
        }
        catch (Throwable e) {
            U.error(log, "Failed to register MBean with name: " + mbeanName, e);
        }
    }

    /**
     * @param cfg Ignite configuration.
     * @param groupName Name of group.
     * @param name Name of MBean.
     */
    protected void unregisterMetricsMBean(
        IgniteConfiguration cfg,
        String groupName,
        String name
    ) {
        if (U.IGNITE_MBEANS_DISABLED)
            return;

        assert cfg != null;

        try {
            cfg.getMBeanServer().unregisterMBean(
                U.makeMBeanName(
                    cfg.getIgniteInstanceName(),
                    groupName,
                    name
                ));
        }
        catch (InstanceNotFoundException ignored) {
            // We tried to unregister a non-existing MBean, not a big deal.
        }
        catch (Throwable e) {
            U.error(log, "Failed to unregister MBean for memory metrics: " + name, e);
        }
    }
}
