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

package org.apache.ignite.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.channels.FileLock;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheTryPutFailedException;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.PluginProvider;

/**
 * Marshaller context implementation.
 */
public class MarshallerContextImpl extends MarshallerContextAdapter {
    /** */
    private static final GridStripedLock fileLock = new GridStripedLock(32);

    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** */
    private final File workDir;

    /** */
    private final String cacheName;

    /** */
    private final String fileExt;

    /** */
    private IgniteLogger log;

    /** */
    private volatile GridCacheAdapter<Integer, String> cache;

    /** Non-volatile on purpose. */
    private int failedCnt;

    /**
     * @param plugins Plugins.
     * @throws IgniteCheckedException In case of error.
     */
    public MarshallerContextImpl(List<PluginProvider> plugins) throws IgniteCheckedException {
        this(plugins, CU.MARSH_CACHE_NAME, ".classname");
    }

    /**
     * @param plugins Plugins.
     * @param cacheName Cache name.
     * @throws IgniteCheckedException In case of error.
     */
    public MarshallerContextImpl(List<PluginProvider> plugins, String cacheName, String fileExt)
        throws IgniteCheckedException {
        super(plugins);

        assert cacheName != null;
        assert fileExt != null;

        workDir = U.resolveWorkDirectory("marshaller", false);
        this.cacheName = cacheName;
        this.fileExt = fileExt;
    }

    /**
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of error.
     */
    public void onMarshallerCacheStarted(final GridKernalContext ctx) throws IgniteCheckedException {
        GridCacheContext<Object, Object> cacheCtx = ctx.cache().internalCache(cacheName).context();

        cacheCtx.continuousQueries().executeInternalQuery(
            new ContinuousQueryListener(ctx.log(MarshallerContextImpl.class), workDir, fileExt),
            null,
            cacheCtx.affinityNode(),
            true,
            false
        );

        cacheCtx.preloader().initialRebalanceFuture().listen(new CIX1<IgniteInternalFuture<?>>() {
            @Override public void applyx(IgniteInternalFuture<?> f) {
                log = ctx.log(MarshallerContextImpl.class);

                cache = ctx.cache().internalCache(cacheName);

                latch.countDown();
            }
        });
    }


    /**
     * Release marshaller context.
     */
    public void onKernalStop() {
        latch.countDown();
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassName(int id, String clsName) throws IgniteCheckedException {
        GridCacheAdapter<Integer, String> cache0 = cache;

        if (cache0 == null)
            return false;

        String old;

        try {
            old = cache0.tryPutIfAbsent(id, clsName);

            if (old != null && !old.equals(clsName))
                throw new IgniteCheckedException("Type ID collision detected [id=" + id + ", clsName1=" + clsName +
                    ", clsName2=" + old + ']');

            failedCnt = 0;

            return true;
        }
        catch (CachePartialUpdateCheckedException | GridCacheTryPutFailedException e) {
            if (++failedCnt > 10) {
                if (log.isQuiet())
                    U.quiet(false, "Failed to register marshalled class for more than 10 times in a row " +
                        "(may affect performance).");

                failedCnt = 0;
            }

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public String className(int id) throws IgniteCheckedException {
        GridCacheAdapter<Integer, String> cache0 = cache;

        if (cache0 == null) {
            U.awaitQuiet(latch);

            cache0 = cache;

            if (cache0 == null)
                throw new IllegalStateException("Failed to initialize marshaller context (grid is stopping).");
        }

        String clsName = cache0.getTopologySafe(id);

        if (clsName == null) {
            String fileName = id + fileExt;

            Lock lock = fileLock(fileName);

            lock.lock();

            try {
                File file = new File(workDir, fileName);

                try (FileInputStream in = new FileInputStream(file)) {
                    FileLock fileLock = in.getChannel().lock(0L, Long.MAX_VALUE, true);

                    assert fileLock != null : fileName;

                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                        clsName = reader.readLine();
                    }
                }
                catch (IOException e) {
                    throw new IgniteCheckedException("Failed to read class name from file [id=" + id +
                        ", file=" + file.getAbsolutePath() + ']', e);
                }
            }
            finally {
                lock.unlock();
            }

            // Must explicitly put entry to cache to invoke other continuous queries.
            registerClassName(id, clsName);
        }

        return clsName;
    }

    /**
     * @param fileName File name.
     * @return Lock instance.
     */
    private static Lock fileLock(String fileName) {
        return fileLock.getLock(fileName.hashCode());
    }

    /**
     */
    private static class ContinuousQueryListener implements CacheEntryUpdatedListener<Integer, String> {
        /** */
        private final IgniteLogger log;

        /** */
        private final File workDir;

        /** */
        private final String fileExt;

        /**
         * @param log Logger.
         * @param workDir Work directory.
         * @param fileExt File extension.
         */
        private ContinuousQueryListener(IgniteLogger log, File workDir, String fileExt) {
            this.log = log;
            this.workDir = workDir;
            this.fileExt = fileExt;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts)
            throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends Integer, ? extends String> evt : evts) {
                assert evt.getOldValue() == null || F.eq(evt.getOldValue(), evt.getValue()):
                    "Received cache entry update for system marshaller cache: " + evt;

                if (evt.getOldValue() == null) {
                    String fileName = evt.getKey() + fileExt;

                    Lock lock = fileLock(fileName);

                    lock.lock();

                    try {
                        File file = new File(workDir, fileName);

                        try (FileOutputStream out = new FileOutputStream(file)) {
                            FileLock fileLock = out.getChannel().lock(0L, Long.MAX_VALUE, false);

                            assert fileLock != null : fileName;

                            try (Writer writer = new OutputStreamWriter(out)) {
                                writer.write(evt.getValue());

                                writer.flush();
                            }
                        }
                        catch (IOException e) {
                            U.error(log, "Failed to write class name to file [id=" + evt.getKey() +
                                ", clsName=" + evt.getValue() + ", file=" + file.getAbsolutePath() + ']', e);
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
        }
    }
}
