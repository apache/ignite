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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.marshaller.MappingExchangeResult;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingTransport;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/**
 * Marshaller context implementation.
 */
public class MarshallerContextImpl implements MarshallerContext {
    /** */
    private static final String CLS_NAMES_FILE = "META-INF/classnames.properties";

    /** */
    private static final String JDK_CLS_NAMES_FILE = "META-INF/classnames-jdk.properties";

    /** */
    private final Set<String> registeredSysTypes = new HashSet<>();

    /** */
    //TODO replace map with some array implementation to improve performance
    private final ConcurrentMap<Byte, ConcurrentMap<Integer, MappedName>> allCaches = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<Integer, MappedName> dfltCache = new ConcurrentHashMap8<>();

    /** */
    private MarshallerMappingFileStore fileStore;

    /** */
    private ExecutorService execSrvc;

    /** */
    private MarshallerMappingTransport transport;

    /** */
    private boolean isClientNode;

    /**
     * Initializes context.
     *
     * @param plugins Plugins.
     */
    public MarshallerContextImpl(@Nullable List<PluginProvider> plugins) {
        initializeCaches();

        try {
            ClassLoader ldr = U.gridClassLoader();

            Enumeration<URL> urls = ldr.getResources(CLS_NAMES_FILE);

            boolean foundClsNames = false;

            while (urls.hasMoreElements()) {
                processResource(urls.nextElement());

                foundClsNames = true;
            }

            if (!foundClsNames)
                throw new IgniteException("Failed to load class names properties file packaged with ignite binaries " +
                    "[file=" + CLS_NAMES_FILE + ", ldr=" + ldr + ']');

            URL jdkClsNames = ldr.getResource(JDK_CLS_NAMES_FILE);

            if (jdkClsNames == null)
                throw new IgniteException("Failed to load class names properties file packaged with ignite binaries " +
                    "[file=" + JDK_CLS_NAMES_FILE + ", ldr=" + ldr + ']');

            processResource(jdkClsNames);

            checkHasClassName(GridDhtPartitionFullMap.class.getName(), ldr, CLS_NAMES_FILE);
            checkHasClassName(GridDhtPartitionMap2.class.getName(), ldr, CLS_NAMES_FILE);
            checkHasClassName(HashMap.class.getName(), ldr, JDK_CLS_NAMES_FILE);

            if (plugins != null && !plugins.isEmpty()) {
                for (PluginProvider plugin : plugins) {
                    URL pluginClsNames = ldr.getResource("META-INF/" + plugin.name().toLowerCase()
                        + ".classnames.properties");

                    if (pluginClsNames != null)
                        processResource(pluginClsNames);
                }
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to initialize marshaller context.", e);
        }
    }

    /** */
    private void initializeCaches() {
        allCaches.put(JAVA_ID, dfltCache);
    }

    /** */
    public HashMap<Byte, Map<Integer, MappedName>> getCachedMappings() {
        HashMap<Byte, Map<Integer, MappedName>> result = U.newHashMap(allCaches.size());

        for (Map.Entry<Byte, ConcurrentMap<Integer, MappedName>> e0 : allCaches.entrySet()) {

            Map res;

            if (e0.getKey() == JAVA_ID) {
                //filtering out system types from default cache to reduce message size
                res = new HashMap();
                for (Map.Entry<Integer, MappedName> e1 : e0.getValue().entrySet())
                    //TODO get rid of registeredSysTypes as a separate entity as it may be expensive to filter stuff in this way
                    if (!registeredSysTypes.contains(e1.getValue().className()))
                        res.put(e1.getKey(), e1.getValue());
            }
            else
                res = e0.getValue();

            if (!res.isEmpty())
                result.put(e0.getKey(), res);
        }

        return result;
    }

    /**
     * @param platformId Platform id.
     * @param marshallerMapping Marshaller mapping.
     */
    public void onMappingDataReceived(byte platformId, Map<Integer, MappedName> marshallerMapping) {
        ConcurrentMap<Integer, MappedName> platformCache = getCacheFor(platformId);

        for (Map.Entry<Integer, MappedName> e : marshallerMapping.entrySet())
            platformCache.put(e.getKey(), new MappedName(e.getValue().className(), true));
    }

    /**
     * @param clsName Class name.
     * @param ldr Class loader used to get properties file.
     * @param fileName File name.
     */
    private void checkHasClassName(String clsName, ClassLoader ldr, String fileName) {
        if (!dfltCache.containsKey(clsName.hashCode()))
            throw new IgniteException("Failed to read class name from class names properties file. " +
                "Make sure class names properties file packaged with ignite binaries is not corrupted " +
                "[clsName=" + clsName + ", fileName=" + fileName + ", ldr=" + ldr + ']');
    }

    /**
     * @param url Resource URL.
     * @throws IOException In case of error.
     */
    private void processResource(URL url) throws IOException {
        try (InputStream in = url.openStream()) {
            BufferedReader rdr = new BufferedReader(new InputStreamReader(in));

            String line;

            while ((line = rdr.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("#"))
                    continue;

                String clsName = line.trim();

                int typeId = clsName.hashCode();

                MappedName oldClsName;

                if ((oldClsName = dfltCache.put(typeId, new MappedName(clsName, true))) != null) {
                    if (!oldClsName.className().equals(clsName))
                        throw new IgniteException("Duplicate type ID [id=" + typeId + ", oldClsName=" + oldClsName + ", clsName=" + clsName + ']');
                }

                registeredSysTypes.add(clsName);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassName(byte platformId, int typeId, String clsName) throws IgniteCheckedException {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(platformId);

        MappedName mappedName = cache.get(typeId);

        if (mappedName != null) {
            if (!mappedName.className().equals(clsName))
                throw duplicateIdException(platformId, typeId, mappedName.className(), clsName);
            else {
                if (mappedName.accepted())
                    return true;

                if (transport.stopping())
                    return false;

                IgniteInternalFuture<MappingExchangeResult> fut = transport.awaitMappingAcceptance(new MarshallerMappingItem(platformId, typeId, clsName), cache);
                MappingExchangeResult res = fut.get();

                return convertXchRes(res);
            }
        }
        else {
            if (transport.stopping())
                return false;

            IgniteInternalFuture<MappingExchangeResult> fut = transport.proposeMapping(new MarshallerMappingItem(platformId, typeId, clsName), cache);
            MappingExchangeResult res = fut.get();

            return convertXchRes(res);
        }
    }

    /**
     * @param res result of exchange.
     */
    private boolean convertXchRes(MappingExchangeResult res) throws IgniteCheckedException {
        if (res.successful())
            return true;
        else if (res.exchangeDisabled())
            return false;
        else {
            assert res.error() != null;
            throw res.error();
        }
    }

    /**
     * @param platformId Platform id.
     * @param typeId Type id.
     * @param conflictingClsName Conflicting class name.
     * @param clsName Class name.
     */
    private IgniteCheckedException duplicateIdException(byte platformId, int typeId, String conflictingClsName, String clsName) {
        return new IgniteCheckedException("Duplicate ID [platformId="
                + platformId
                + ", typeId="
                + typeId
                + ", oldCls="
                + conflictingClsName
                + ", newCls="
                + clsName + "]");
    }

    /**
     *
     * @param item type mapping to propose
     * @return false if there is a conflict with another mapping in local cache, true otherwise.
     */
    public String onMappingProposed(MarshallerMappingItem item) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(item.platformId());

        MappedName newName = new MappedName(item.className(), false);
        MappedName oldName;
        if ((oldName = cache.putIfAbsent(item.typeId(), newName)) == null)
            return null;
        else
            return oldName.className();
    }

    /**
     * @param item Item.
     */
    public void onMappingAccepted(final MarshallerMappingItem item) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(item.platformId());

        cache.replace(item.typeId(), new MappedName(item.className(), true));

        execSrvc.submit(new MappingStoreTask(fileStore, item));
    }

    /** {@inheritDoc} */
    @Override public Class getClass(int typeId, ClassLoader ldr) throws ClassNotFoundException, IgniteCheckedException {
        String clsName = getClassName(JAVA_ID, typeId);

        if (clsName == null)
            throw new ClassNotFoundException("Unknown type ID: " + typeId);

        return U.forName(clsName, ldr);
    }

    /** {@inheritDoc} */
    @Override public String getClassName(byte platformId, int typeId) throws ClassNotFoundException, IgniteCheckedException {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(platformId);

        String clsName;
        MappedName mappedName = cache.get(typeId);

        if (mappedName != null)
            clsName = mappedName.className();
        else {
            clsName = fileStore.readMapping(platformId, typeId);
            if (clsName != null)
                cache.putIfAbsent(typeId, new MappedName(clsName, true));
            else
                if (isClientNode) {
                    mappedName = cache.get(typeId);
                    if (mappedName == null) {
                        GridFutureAdapter<MappingExchangeResult> fut = transport.requestMapping(new MarshallerMappingItem(platformId, typeId, null), cache);
                        clsName = fut.get().className();
                    }
                    else
                        clsName = cache.get(typeId).className();

                    if (clsName == null)
                        throw new ClassNotFoundException("Requesting mapping from grid failed for [platformId=" + platformId + ", typeId=" + typeId + "]");

                    return clsName;
                }
                else
                    throw new ClassNotFoundException("Unknown pair [platformId= " + platformId + ", typeId=" + typeId + "]");
        }

        return clsName;
    }

    /**
     * @param platformId Platform id.
     * @param typeId Type id.
     */
    public String resolveMissedMapping(byte platformId, int typeId) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(platformId);

        MappedName mappedName = cache.get(typeId);

        if (mappedName != null) {
            assert mappedName.accepted() : mappedName;

            return mappedName.className();
        }

        return null;
    }

    /**
     * @param item Item.
     * @param resolvedClsName Resolved class name.
     */
    public void onMissedMappingResolved(MarshallerMappingItem item, String resolvedClsName) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(item.platformId());

        int typeId = item.typeId();
        MappedName mappedName = cache.get(typeId);

        if (mappedName != null)
            assert resolvedClsName.equals(mappedName.className()) : "Class name resolved from cluster: " + resolvedClsName + ", class name from local cache: " + mappedName.className();
        else {
            mappedName = new MappedName(resolvedClsName, true);
            cache.putIfAbsent(typeId, mappedName);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemType(String typeName) {
        return registeredSysTypes.contains(typeName);
    }

    /**
     * @param platformId Platform id.
     */
    private ConcurrentMap<Integer, MappedName> getCacheFor(byte platformId) {
        ConcurrentMap<Integer, MappedName> map = (platformId == JAVA_ID) ? dfltCache : allCaches.get(platformId);

        if (map != null)
            return map;

        map = new ConcurrentHashMap8<>();
        ConcurrentMap<Integer, MappedName> oldMap = allCaches.putIfAbsent(platformId, map);

        return oldMap != null ? oldMap : map;
    }

    /**
     * @param ctx Context.
     * @param transport Transport.
     */
    public void onMarshallerProcessorStarted(GridKernalContext ctx, MarshallerMappingTransport transport) throws IgniteCheckedException {
        assert ctx != null;

        IgniteConfiguration cfg = ctx.config();
        String workDir = U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome());

        fileStore = new MarshallerMappingFileStore(workDir, ctx.log(MarshallerMappingFileStore.class));
        this.transport = transport;
        execSrvc = ctx.getSystemExecutorService();
        isClientNode = ctx.clientNode();
    }

    /**
     *
     */
    public void onMarshallerProcessorStop() {
        transport.markStopping();
    }
}