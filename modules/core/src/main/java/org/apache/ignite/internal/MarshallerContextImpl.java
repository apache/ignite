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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.marshaller.MappedNameImpl;
import org.apache.ignite.internal.processors.marshaller.MappingRequestFuture;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
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
    private final Set<String> registeredSystemTypes = new HashSet<>();

    private final Map<Byte, ConcurrentMap<Integer, MappedName>> allCaches = new HashMap<>();

    private ConcurrentMap<Integer, MappedName> defaultCache = new ConcurrentHashMap8<>();

    private MarshallerMappingPersistence persistence;

    private MarshallerMappingTransport transport;

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

    private void initializeCaches() {
        allCaches.put(JAVA_ID, defaultCache);
    }

    public HashMap<Byte, ConcurrentMap<Integer, MappedName>> getCachedMappings() {
        HashMap<Byte, ConcurrentMap<Integer, MappedName>> result = new HashMap<>(allCaches.size());

        for (Map.Entry<Byte, ConcurrentMap<Integer, MappedName>> e0 : allCaches.entrySet()) {

            ConcurrentMap<Integer, MappedName> res;

            if (e0.getValue() == defaultCache) {
                //filtering out system types from default cache to reduce message size
                res = new ConcurrentHashMap8<>();
                for (Map.Entry<Integer, MappedName> e1 : e0.getValue().entrySet())
                    if (!registeredSystemTypes.contains(e1.getValue().className()))
                        res.put(e1.getKey(), e1.getValue());
            } else
                res = e0.getValue();

            if (res.size() > 0)
                result.put(e0.getKey(), res);
        }

        return result;
    }

    public void applyPlatformMapping(byte platformId, Map<Integer, MappedName> marshallerMapping) {
        ConcurrentMap<Integer, MappedName> platformCache = getCacheFor(platformId);

        if (platformCache == null) {
            platformCache = new ConcurrentHashMap8<>();
            allCaches.put(platformId, platformCache);
        }

        for (Map.Entry<Integer, MappedName> e : marshallerMapping.entrySet())
            platformCache.putIfAbsent(e.getKey(), e.getValue());
    }

    /**
     * @param clsName Class name.
     * @param ldr Class loader used to get properties file.
     * @param fileName File name.
     */
    private void checkHasClassName(String clsName, ClassLoader ldr, String fileName) {
        if (!defaultCache.containsKey(clsName.hashCode()))
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

                MappedNameImpl oldClsName;

                if ((oldClsName = (MappedNameImpl) defaultCache.put(typeId, new MappedNameImpl(clsName, true))) != null) {
                    if (!oldClsName.className().equals(clsName))
                        throw new IgniteException("Duplicate type ID [id=" + typeId + ", clsName=" + clsName +
                        ", oldClsName=" + oldClsName + ']');
                }

                registeredSystemTypes.add(clsName);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassName(byte platformId, int typeId, String clsName) throws IgniteCheckedException {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(platformId);

        MappedName mappedName = cache.get(typeId);

        if (mappedName != null)
            if (mappedName instanceof MappingRequestFuture)
                return false;
            else if (!mappedName.className().equals(clsName))
                throw new IgniteCheckedException("Duplicate ID [id="
                        + typeId
                        + ", oldCls="
                        + mappedName.className()
                        + ", newCls="
                        + clsName);
            else
                return mappedName.isAccepted();
        else {
            transport.proposeMapping(platformId, typeId, clsName);
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean registerMappingForPlatform(byte newPlatformId) {
        if (allCaches.containsKey(newPlatformId))
            return false;
        else
            allCaches.put(newPlatformId, new ConcurrentHashMap8<Integer, MappedName>());
        return true;
    }

    /**
     *
     * @param item type mapping to propose
     * @return false if there is a conflict with another mapping in local cache, true otherwise.
     */
    public boolean onMappingProposed(MarshallerMappingItem item) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(item.getPlatformId());

        if (cache.putIfAbsent(item.getTypeId(), new MappedNameImpl(item.getClassName(), false)) == null)
            return true;
        else
            return false;
    }

    public void onMappingAccepted(MarshallerMappingItem item) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(item.getPlatformId());

        cache.replace(item.getTypeId(), new MappedNameImpl(item.getClassName(), true));

        persistence.onMappingAccepted(item.getPlatformId(), item.getTypeId(), item.getClassName());
    }

    /** {@inheritDoc} */
    @Override public Class getClass(byte platformId, int typeId, ClassLoader ldr) throws ClassNotFoundException, IgniteCheckedException {
        String clsName = getClassName(platformId, typeId);

        if (clsName == null)
            throw new ClassNotFoundException("Unknown type ID: " + typeId);

        return U.forName(clsName, ldr);
    }

    @Override public String getClassName(byte platformId, int typeId) throws ClassNotFoundException, IgniteCheckedException {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(platformId);

        if (cache == null)
            throw new IgniteCheckedException("Cache for platformId=" + platformId + " not found.");

        String clsName;
        MappedName mappedName = cache.get(typeId);

        if (mappedName != null)
            clsName = mappedName.className();
        else {
            clsName = persistence.onMappingMiss(platformId, typeId);
            if (clsName != null)
                cache.putIfAbsent(typeId, new MappedNameImpl(clsName, true));
            else
                if (isClientNode) {
                    mappedName = cache.get(typeId);
                    if (mappedName == null) {
                        mappedName = transport.requestMapping(platformId, typeId);
                        cache.putIfAbsent(typeId, mappedName);
                    }

                    clsName = cache.get(typeId).className();

                    if (clsName == null)
                        throw new ClassNotFoundException("Requesting mapping from grid failed for type ID: " + typeId);

                    return clsName;

                    //also take into account that Future objects should never be serialized and sent across the ring
                } else
                    throw new ClassNotFoundException("Unknown type ID: " + typeId);
        }

        return clsName;
    }

    public boolean resolveMissedMapping(MarshallerMappingItem item) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(item.getPlatformId());

        if (cache != null) {
            MappedName mappedName = cache.get(item.getTypeId());

            if (mappedName != null && mappedName.className() != null) {
                item.setClassName(mappedName.className());
                try {
                    Thread.sleep(750);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return true;
            }
        }

        return false;
    }

    public void onMissedMappingResolved(MarshallerMappingItem item) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(item.getPlatformId());

        if (cache != null) {
            int typeId = item.getTypeId();
            MappedName mappedName = cache.get(typeId);

            if (mappedName instanceof MappingRequestFuture) {
                String clsName = item.getClassName();
                if (clsName != null) {
                    MappedName newMappedName = new MappedNameImpl(clsName, true);

                    cache.replace(typeId, newMappedName);

                    ((MappingRequestFuture) mappedName).onMappingResolved(newMappedName);
                } else
                    ((MappingRequestFuture) mappedName).onMappingResolutionFailed();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemType(String typeName) {
        return registeredSystemTypes.contains(typeName);
    }

    private ConcurrentMap<Integer, MappedName> getCacheFor(byte platformId) {
        return (platformId == JAVA_ID) ? defaultCache : allCaches.get(platformId);
    }

    public void onMarshallerProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        persistence = new MarshallerMappingPersistence(ctx.log(MarshallerMappingPersistence.class));
        transport = new MarshallerMappingTransport(ctx.discovery());
        isClientNode = ctx.clientNode();
    }
}