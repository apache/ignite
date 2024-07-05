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

package org.apache.ignite.internal.management.cache.scan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

import static java.lang.Math.min;
import static org.apache.ignite.cache.query.Query.DFLT_PAGE_SIZE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;

/**
 * Task that scan cache entries.
 */
@GridInternal
public class CacheScanTask extends VisorOneNodeTask<CacheScanCommandArg, CacheScanTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected CacheScanJob job(CacheScanCommandArg arg) {
        return new CacheScanJob(arg, debug);
    }

    /**
     * Job that stop specified caches.
     */
    private static class CacheScanJob extends VisorJob<CacheScanCommandArg, CacheScanTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private CacheScanJob(CacheScanCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected CacheScanTaskResult run(CacheScanCommandArg arg) {
            if (F.isEmpty(arg.cacheName()))
                throw new IllegalStateException("Cache name was not specified.");

            if (arg.limit() <= 0)
                throw new IllegalStateException("Invalid limit value.");

            IgniteCache<Object, Object> cache = ignite.cache(arg.cacheName()).withKeepBinary();

            int cnt = 0;
            List<List<?>> entries = new ArrayList<>();

            CacheScanTaskFormat format = format(arg.outputFormat());

            List<String> titles = Collections.emptyList();

            ScanQuery<Object, Object> scanQry = new ScanQuery<>().setPageSize(min(arg.limit(), DFLT_PAGE_SIZE));

            try (QueryCursor<Cache.Entry<Object, Object>> qry = cache.query(scanQry)) {
                Iterator<Cache.Entry<Object, Object>> iter = qry.iterator();

                if (cnt++ < arg.limit() && iter.hasNext()) {
                    Cache.Entry<Object, Object> first = iter.next();

                    titles = format.titles(first);

                    entries.add(format.row(first));
                }

                while (cnt++ < arg.limit() && iter.hasNext()) {
                    entries.add(format.row(iter.next()));
                }
            }

            return new CacheScanTaskResult(titles, entries);
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            // This task does nothing but delegates the call to the Ignite public API.
            // Therefore, it is safe to execute task without any additional permissions check.
            return NO_PERMISSIONS;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheScanJob.class, this);
        }
    }

    /** */
    private static CacheScanTaskFormat format(String format) {
        if (format == null)
            return new DefaultCacheScanTaskFormat();

        Iterable<CacheScanTaskFormat> formats = U.loadService(CacheScanTaskFormat.class);

        for (CacheScanTaskFormat f : formats) {
            if (f.name().equals(format))
                return f;
        }

        throw new IllegalStateException("Unknown format: " + format);
    }
}
