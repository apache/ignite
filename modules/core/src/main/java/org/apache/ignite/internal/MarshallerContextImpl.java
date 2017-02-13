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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.marshaller.MappingExchangeResult;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingTransport;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.NotNull;
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
    private final Map<Integer, MappedName> sysTypesMap = new HashMap<>();

    /** */
    private final Collection<String> sysTypesSet = new HashSet<>();

    /** */
    private final List<ConcurrentMap<Integer, MappedName>> allCaches = new CopyOnWriteArrayList<>();

    /** */
    private MarshallerMappingFileStore fileStore;

    /** */
    private GridClosureProcessor closProc;

    /** */
    private MarshallerMappingTransport transport;

    /** */
    private boolean clientNode;

    /**
     * Initializes context.
     *
     * @param plugins Plugins.
     */
    public MarshallerContextImpl(@Nullable Collection<PluginProvider> plugins) {
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
        allCaches.add(new CombinedMap(new ConcurrentHashMap8<Integer, MappedName>(), sysTypesMap));
    }

    /** */
    public ArrayList<Map<Integer, MappedName>> getCachedMappings() {
        ArrayList<Map<Integer, MappedName>> result = new ArrayList<>(allCaches.size());

        for (int i = 0; i < allCaches.size(); i++) {
            Map res;

            if (i == JAVA_ID)
                res = ((CombinedMap) allCaches.get(JAVA_ID)).userMap;
            else
                res = allCaches.get(i);

            if (!res.isEmpty())
                result.add(res);
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
    public void checkHasClassName(String clsName, ClassLoader ldr, String fileName) {
        ConcurrentMap cache = getCacheFor(JAVA_ID);

        if (!cache.containsKey(clsName.hashCode()))
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

                if ((oldClsName = sysTypesMap.put(typeId, new MappedName(clsName, true))) != null) {
                    if (!oldClsName.className().equals(clsName))
                        throw new IgniteException(
                                "Duplicate type ID [id="
                                        + typeId
                                        + ", oldClsName="
                                        + oldClsName
                                        + ", clsName="
                                        + clsName + ']');
                }

                sysTypesSet.add(clsName);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassName(
            byte platformId,
            int typeId,
            String clsName
    ) throws IgniteCheckedException {
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
    private IgniteCheckedException duplicateIdException(
            byte platformId,
            int typeId,
            String conflictingClsName,
            String clsName
    ) {
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

        closProc.runLocalSafe(new MappingStoreTask(fileStore, item.platformId(), item.typeId(), item.className()));
    }

    /** {@inheritDoc} */
    @Override public Class getClass(int typeId, ClassLoader ldr) throws ClassNotFoundException, IgniteCheckedException {
        String clsName = getClassName(JAVA_ID, typeId);

        if (clsName == null)
            throw new ClassNotFoundException("Unknown type ID: " + typeId);

        return U.forName(clsName, ldr);
    }

    /** {@inheritDoc} */
    @Override public String getClassName(
            byte platformId,
            int typeId
    ) throws ClassNotFoundException, IgniteCheckedException {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(platformId);

        MappedName mappedName = cache.get(typeId);

        String clsName;

        if (mappedName != null)
            clsName = mappedName.className();
        else {
            clsName = fileStore.readMapping(platformId, typeId);

            if (clsName != null)
                cache.putIfAbsent(typeId, new MappedName(clsName, true));
            else
                if (clientNode) {
                    mappedName = cache.get(typeId);

                    if (mappedName == null) {
                        GridFutureAdapter<MappingExchangeResult> fut = transport.requestMapping(
                                new MarshallerMappingItem(platformId, typeId, null),
                                cache);

                        clsName = fut.get().className();
                    }
                    else
                        clsName = mappedName.className();

                    if (clsName == null)
                        throw new ClassNotFoundException(
                                "Requesting mapping from grid failed for [platformId="
                                        + platformId
                                        + ", typeId="
                                        + typeId + "]");

                    return clsName;
                }
                else
                    throw new ClassNotFoundException(
                            "Unknown pair [platformId="
                                    + platformId
                                    + ", typeId="
                                    + typeId + "]");
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
    public void onMissedMappingResolved(final MarshallerMappingItem item, String resolvedClsName) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(item.platformId());

        int typeId = item.typeId();
        MappedName mappedName = cache.get(typeId);

        if (mappedName != null)
            assert resolvedClsName.equals(mappedName.className()) :
                    "Class name resolved from cluster: "
                            + resolvedClsName
                            + ", class name from local cache: "
                            + mappedName.className();
        else {
            mappedName = new MappedName(resolvedClsName, true);
            cache.putIfAbsent(typeId, mappedName);

            closProc.runLocalSafe(new MappingStoreTask(fileStore, item.platformId(), item.typeId(), resolvedClsName));
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemType(String typeName) {
        return sysTypesSet.contains(typeName);
    }

    /**
     * @param platformId Platform id.
     */
    private ConcurrentMap<Integer, MappedName> getCacheFor(byte platformId) {
        ConcurrentMap<Integer, MappedName> map;

        if (platformId < allCaches.size()) {
            map = allCaches.get(platformId);

            if (map != null)
                return map;
        }

        synchronized (this) {
            int size = allCaches.size();

            if (platformId < size) {
                map = allCaches.get(platformId);

                if (map == null) {
                    map = new ConcurrentHashMap8<>();
                    allCaches.set(platformId, map);
                }
            }
            else {
                map = new ConcurrentHashMap8<>();

                putAtIndex(map, allCaches, platformId, size);
            }
        }

        return map;
    }

    /**
     * @param map Map.
     * @param allCaches All caches.
     * @param targetIdx Target index.
     * @param size Size.
     */
    private static void putAtIndex(
            ConcurrentMap<Integer, MappedName> map,
            Collection<ConcurrentMap<Integer, MappedName>> allCaches,
            byte targetIdx,
            int size
    ) {
        int lastIdx = size - 1;

        int nullElemsToAdd = targetIdx - lastIdx - 1;

        for (int i = 0; i < nullElemsToAdd; i++)
            allCaches.add(null);

        allCaches.add(map);
    }

    /**
     * @param ctx Context.
     * @param transport Transport.
     */
    public void onMarshallerProcessorStarted(
            GridKernalContext ctx,
            MarshallerMappingTransport transport
    ) throws IgniteCheckedException {
        assert ctx != null;

        IgniteConfiguration cfg = ctx.config();
        String workDir = U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome());

        fileStore = new MarshallerMappingFileStore(workDir, ctx.log(MarshallerMappingFileStore.class));
        this.transport = transport;
        closProc = ctx.closure();
        clientNode = ctx.clientNode();
    }

    /**
     *
     */
    public void onMarshallerProcessorStop() {
        transport.markStopping();
    }

    /**
     * Method collects current mappings for all platforms.
     *
     * @return current mappings.
     */
    public Iterator<Map.Entry<Byte, Map<Integer, String>>> currentMappings() {
        int size = allCaches.size();

        Map<Byte, Map<Integer, String>> res = IgniteUtils.newHashMap(size);

        for (byte i = 0; i < size; i++) {
            Map<Integer, MappedName> platformMappings = allCaches.get(i);

            if (platformMappings != null) {
                if (i == JAVA_ID)
                    platformMappings = ((CombinedMap)platformMappings).userMap;

                Map<Integer, String> nameMappings = IgniteUtils.newHashMap(platformMappings.size());

                for (Map.Entry<Integer, MappedName> e : platformMappings.entrySet())
                    nameMappings.put(e.getKey(), e.getValue().className());

                res.put(i, nameMappings);
            }
        }

        return res.entrySet().iterator();
    }

    /**
     *
     */
    static final class CombinedMap extends AbstractMap<Integer, MappedName>
            implements ConcurrentMap<Integer, MappedName> {
        /** */
        private final ConcurrentMap<Integer, MappedName> userMap;

        /** */
        private final Map<Integer, MappedName> sysMap;

        /**
         * @param userMap User map.
         * @param sysMap System map.
         */
        CombinedMap(ConcurrentMap<Integer, MappedName> userMap, Map<Integer, MappedName> sysMap) {
            this.userMap = userMap;
            this.sysMap = sysMap;
        }

        /** {@inheritDoc} */
        @Override public Set<Entry<Integer, MappedName>> entrySet() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public MappedName putIfAbsent(@NotNull Integer key, MappedName val) {
            return userMap.putIfAbsent(key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean remove(@NotNull Object key, Object val) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean replace(@NotNull Integer key, @NotNull MappedName oldVal, @NotNull MappedName newVal) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public MappedName replace(@NotNull Integer key, @NotNull MappedName val) {
            return userMap.replace(key, val);
        }

        /** {@inheritDoc} */
        @Override public MappedName get(Object key) {
            MappedName res = sysMap.get(key);

            if (res != null)
                return res;

            return userMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public MappedName put(Integer key, MappedName val) {
            return userMap.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(Object key) {
            return userMap.containsKey(key) || sysMap.containsKey(key);
        }
    }
}