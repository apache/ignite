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

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiPredicate;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.marshaller.MappingExchangeResult;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingTransport;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;
import static org.apache.ignite.internal.MarshallerPlatformIds.otherPlatforms;
import static org.apache.ignite.internal.MarshallerPlatformIds.platformName;
import static org.apache.ignite.marshaller.MarshallerUtils.CLS_NAMES_FILE;
import static org.apache.ignite.marshaller.MarshallerUtils.JDK_CLS_NAMES_FILE;

/**
 * Marshaller context implementation.
 */
public class MarshallerContextImpl implements MarshallerContext {
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

    /** Class name filter. */
    private final IgnitePredicate<String> clsFilter;

    /** JDK marshaller. */
    private final JdkMarshaller jdkMarsh;

    /**
     * Marshaller mapping file store directory. {@code null} used for standard folder, in this case folder is calculated
     * from work directory. Non null value may be used to setup custom directory from outside
     */
    @Nullable private File marshallerMappingFileStoreDir;

    /**
     * Initializes context.
     *
     * @param plugins Plugins.
     */
    public MarshallerContextImpl(@Nullable Collection<PluginProvider> plugins, IgnitePredicate<String> clsFilter) {
        this.clsFilter = clsFilter;
        this.jdkMarsh = new JdkMarshaller(clsFilter);

        initializeCaches();

        try {
            ClassLoader ldr = U.gridClassLoader();

            MarshallerUtils.processSystemClasses(ldr, plugins, clsName -> {
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
            });

            checkHasClassName(GridDhtPartitionFullMap.class.getName(), ldr, CLS_NAMES_FILE);
            checkHasClassName(GridDhtPartitionMap.class.getName(), ldr, CLS_NAMES_FILE);
            checkHasClassName(HashMap.class.getName(), ldr, JDK_CLS_NAMES_FILE);
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to initialize marshaller context.", e);
        }
    }

    /** */
    private void initializeCaches() {
        allCaches.add(new CombinedMap(new ConcurrentHashMap<Integer, MappedName>(), sysTypesMap));
    }

    /** */
    public ArrayList<Map<Integer, MappedName>> getCachedMappings() {
        int size = allCaches.size();

        ArrayList<Map<Integer, MappedName>> result = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            Map res;

            if (i == JAVA_ID)
                res = ((CombinedMap) allCaches.get(JAVA_ID)).userMap;
            else
                res = allCaches.get(i);

            if (res != null && !res.isEmpty())
                result.add(res);
            else
                result.add(Collections.<Integer, MappedName>emptyMap());
        }

        return result;
    }

    /**
     * @param log Ignite logger.
     * @param mappings All marshaller mappings to write.
     */
    public void onMappingDataReceived(IgniteLogger log, List<Map<Integer, MappedName>> mappings) {
        addPlatformMappings(log,
            mappings,
            this::getCacheFor,
            (mappedName, clsName) ->
                mappedName == null || F.isEmpty(clsName) || !clsName.equals(mappedName.className()),
            fileStore);
    }

    /**
     * @param ctx Kernal context.
     * @param mappings Marshaller mappings to save.
     * @param dir Directory to save given mappings to.
     */
    public static void saveMappings(GridKernalContext ctx, List<Map<Integer, MappedName>> mappings, File dir)
        throws IgniteCheckedException {
        MarshallerMappingFileStore writer = new MarshallerMappingFileStore(ctx,
            resolveMappingFileStoreWorkDir(dir.getAbsolutePath()));

        addPlatformMappings(ctx.log(MarshallerContextImpl.class),
            mappings,
            b -> new ConcurrentHashMap<>(),
            (mappedName, clsName) -> true,
            writer);
    }

    /**
     * @param mappings Map of marshaller mappings.
     * @param mappedCache Cache to attach new mappings to.
     * @param cacheAddPred Check mapping can be added.
     * @param writer Persistence mapping writer.
     */
    private static void addPlatformMappings(
        IgniteLogger log,
        List<Map<Integer, MappedName>> mappings,
        Function<Byte, ConcurrentMap<Integer, MappedName>> mappedCache,
        BiPredicate<MappedName, String> cacheAddPred,
        MarshallerMappingFileStore writer
    ) {
        if (mappings == null)
            return;

        for (byte platformId = 0; platformId < mappings.size(); platformId++) {
            Map<Integer, MappedName> attach = mappings.get(platformId);

            if (attach == null)
                continue;

            ConcurrentMap<Integer, MappedName> cached = mappedCache.apply(platformId);

            for (Map.Entry<Integer, MappedName> e : attach.entrySet()) {
                Integer typeId = e.getKey();
                String clsName = e.getValue().className();

                if (cacheAddPred.test(cached.get(typeId), clsName)) {
                    try {
                        cached.put(typeId, new MappedName(clsName, true));

                        writer.mergeAndWriteMapping(platformId, typeId, clsName);
                    }
                    catch (IgniteCheckedException ex) {
                        U.error(log, "Failed to write marshaller mapping data", ex);
                    }
                }
            }
        }
    }

    /**
     * @param clsName Class name.
     * @param ldr Class loader used to get properties file.
     * @param fileName File name.
     */
    public void checkHasClassName(String clsName, ClassLoader ldr, String fileName) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(JAVA_ID);

        if (!cache.containsKey(clsName.hashCode()))
            throw new IgniteException("Failed to read class name from class names properties file. " +
                "Make sure class names properties file packaged with ignite binaries is not corrupted " +
                "[clsName=" + clsName + ", fileName=" + fileName + ", ldr=" + ldr + ']');
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassName(
        byte platformId,
        int typeId,
        String clsName,
        boolean failIfUnregistered
    ) throws IgniteCheckedException {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(platformId);

        MappedName mappedName = cache.get(typeId);

        if (mappedName != null) {
            if (!mappedName.className().equals(clsName))
                throw new DuplicateTypeIdException(platformId, typeId, mappedName.className(), clsName);
            else {
                if (mappedName.accepted())
                    return true;

                if (transport.stopping())
                    return false;

                MarshallerMappingItem item = new MarshallerMappingItem(platformId, typeId, clsName);

                GridFutureAdapter<MappingExchangeResult> fut = transport.awaitMappingAcceptance(item, cache);

                if (failIfUnregistered && !fut.isDone())
                    throw new UnregisteredBinaryTypeException(typeId, fut);

                MappingExchangeResult res = fut.get();

                return convertXchRes(res);
            }
        }
        else {
            if (transport.stopping())
                return false;

            MarshallerMappingItem item = new MarshallerMappingItem(platformId, typeId, clsName);

            GridFutureAdapter<MappingExchangeResult> fut = transport.proposeMapping(item, cache);

            if (failIfUnregistered && !fut.isDone())
                throw new UnregisteredBinaryTypeException(typeId, fut);

            MappingExchangeResult res = fut.get();

            return convertXchRes(res);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassName(byte platformId, int typeId, String clsName)
        throws IgniteCheckedException {
        return registerClassName(platformId, typeId, clsName, false);
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassNameLocally(byte platformId, int typeId, String clsName)
        throws IgniteCheckedException
    {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(platformId);

        fileStore.mergeAndWriteMapping(platformId, typeId, clsName);

        cache.put(typeId, new MappedName(clsName, true));

        return true;
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
     *
     * @param item type mapping to propose
     * @return null if cache doesn't contain any mappings for given (platformId, typeId) pair,
     * previous {@link MappedName mapped name} otherwise.
     */
    public MappedName onMappingProposed(MarshallerMappingItem item) {
        ConcurrentMap<Integer, MappedName> cache = getCacheFor(item.platformId());

        MappedName newName = new MappedName(item.className(), false);

        return cache.putIfAbsent(item.typeId(), newName);
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

        return U.forName(clsName, ldr, clsFilter);
    }

    /** {@inheritDoc} */
    @Override public String getClassName(
            byte platformId,
            int typeId
    ) throws ClassNotFoundException, IgniteCheckedException {
        return getClassName(platformId, typeId, false);
    }

    /**
     * Gets class name for provided (platformId, typeId) pair.
     *
     * @param platformId id of a platform the class was registered for.
     * @param typeId Type ID.
     * @param skipOtherPlatforms Whether to skip other platforms check (recursion guard).
     * @return Class name
     * @throws ClassNotFoundException If class was not found.
     * @throws IgniteCheckedException In case of any other error.
     */
    private String getClassName(
            byte platformId,
            int typeId,
            boolean skipOtherPlatforms
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
            else if (clientNode) {
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
            else {
                String platformName = platformName(platformId);

                if (!skipOtherPlatforms) {
                    // Look for this class in other platforms to provide a better error message.
                    for (byte otherPlatformId : otherPlatforms(platformId)) {
                        try {
                            clsName = getClassName(otherPlatformId, typeId, true);
                        } catch (ClassNotFoundException ignored) {
                            continue;
                        }

                        String otherPlatformName = platformName(otherPlatformId);

                        throw new ClassNotFoundException(
                                "Failed to resolve " + otherPlatformName + " class '" + clsName
                                        + "' in " + platformName
                                        + " [platformId=" + platformId
                                        + ", typeId=" + typeId + "].");
                    }
                }

                throw new ClassNotFoundException(
                        "Failed to resolve class name [" +
                                "platformId=" + platformId
                                + ", platform=" + platformName
                                + ", typeId=" + typeId + "]");
            }
        }

        return clsName;
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<String> classNameFilter() {
        return clsFilter;
    }

    /** {@inheritDoc} */
    @Override public JdkMarshaller jdkMarshaller() {
        return jdkMarsh;
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
                    map = new ConcurrentHashMap<>();
                    allCaches.set(platformId, map);
                }
            }
            else {
                map = new ConcurrentHashMap<>();

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

        fileStore = marshallerMappingFileStoreDir == null ?
            new MarshallerMappingFileStore(ctx, resolveMappingFileStoreWorkDir(workDir)) :
            new MarshallerMappingFileStore(ctx, marshallerMappingFileStoreDir);
        this.transport = transport;
        closProc = ctx.closure();
        clientNode = ctx.clientNode();

        if (CU.isPersistenceEnabled(ctx.config()))
            fileStore.restoreMappings(this);
    }

    /**
     * @param igniteWorkDir Base ignite working directory.
     * @return Resolved directory.
     */
    public static File resolveMappingFileStoreWorkDir(String igniteWorkDir) {
        File dir = mappingFileStoreWorkDir(igniteWorkDir);

        if (!U.mkdirs(dir))
            throw new IgniteException("Could not create directory for marshaller mappings: " + dir);

        return dir;
    }

    /**
     * @param igniteWorkDir Base ignite working directory.
     * @return Work directory for marshaller mappings.
     */
    public static File mappingFileStoreWorkDir(String igniteWorkDir) {
        if (F.isEmpty(igniteWorkDir))
            throw new IgniteException("Work directory has not been set: " + igniteWorkDir);

        return new File(igniteWorkDir, DataStorageConfiguration.DFLT_MARSHALLER_PATH);
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
     * @return {@code True} if marshaller context is initialized.
     */
    public boolean initialized() {
        return fileStore != null;
    }

    /**
     * Sets custom marshaller mapping files directory. Used for standalone WAL iteration
     *
     * @param marshallerMappingFileStoreDir directory with type name mappings
     */
    public void setMarshallerMappingFileStoreDir(@Nullable final File marshallerMappingFileStoreDir) {
        this.marshallerMappingFileStoreDir = marshallerMappingFileStoreDir;
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
            return Collections.emptySet();
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
