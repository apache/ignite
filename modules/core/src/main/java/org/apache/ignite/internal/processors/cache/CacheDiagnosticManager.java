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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
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
    private PageLockTrackerManager pageLockTrackerManager;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        String name = U.maskForFileName(cctx.kernalContext().pdsFolderResolver().resolveFolders().consistentId().toString());

        pageLockTrackerManager = new PageLockTrackerManager(log, name);

        pageLockTrackerManager.start();

        if (cctx.kernalContext().mBeans().isEnabled()) {
            cctx.kernalContext().mBeans().registerMBean(
                MBEAN_GROUP,
                PageLockTrackerMXBean.MBEAN_NAME,
                pageLockTrackerManager.mxBean(),
                PageLockTrackerMXBean.class
            );
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        pageLockTrackerManager.stop();
    }

    /**
     * Getter.
     *
     * @return Page lock tracker manager.
     */
    public PageLockTrackerManager pageLockTracker() {
        return pageLockTrackerManager;
    }
}
