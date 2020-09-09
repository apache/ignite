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

package org.apache.ignite.internal.processors.cache.binary;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.cache.CacheException;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.UnregisteredBinaryTypeException;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOffheapInputStream;
import org.apache.ignite.internal.managers.systemview.walker.BinaryMetadataViewWalker;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheDefaultBinaryAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.IncompleteCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.cacheobject.UserCacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cacheobject.UserCacheObjectImpl;
import org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;
import org.apache.ignite.spi.systemview.view.BinaryMetadataView;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAIT_SCHEMA_UPDATE;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.BINARY_PROC;
import static org.apache.ignite.internal.binary.BinaryUtils.mergeMetadata;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Binary processor implementation.
 */
public class CacheObjectBinaryProcessorImpl extends GridProcessorAdapter implements IgniteCacheObjectProcessor {
    /** Immutable classes. */
    private static final Collection<Class<?>> IMMUTABLE_CLS = new HashSet<>();

    /** @see BinaryMetadataView */
    public static final String BINARY_METADATA_VIEW = metricName("binary", "metadata");

    /** @see BinaryMetadataView */
    public static final String BINARY_METADATA_DESC = "Binary metadata";

    /** */
    private volatile boolean discoveryStarted;

    /** */
    private volatile IgniteFuture<?> reconnectFut;

    /** */
    private BinaryContext binaryCtx;

    /** */
    private Marshaller marsh;

    /** */
    private GridBinaryMarshaller binaryMarsh;

    /** */
    private BinaryMetadataFileStore metadataFileStore;

    /**
     * Custom folder specifying local folder for {@link #metadataFileStore}.<br>
     * {@code null} means no specific folder is configured. <br>
     * In this case folder for metadata is composed from work directory and consistentId <br>
     */
    @Nullable private File binaryMetadataFileStoreDir;

    /** How long to wait for schema if no updates in progress. */
    private long waitSchemaTimeout = IgniteSystemProperties.getLong(IGNITE_WAIT_SCHEMA_UPDATE, 30_000);

    /** For tests. */
    @SuppressWarnings("PublicField")
    public static boolean useTestBinaryCtx;

    /** */
    @GridToStringExclude
    private IgniteBinary binaries;

    /** Locally cached metadata. This local cache is managed by exchanging discovery custom events. */
    private final ConcurrentMap<Integer, BinaryMetadataHolder> metadataLocCache = new ConcurrentHashMap<>();

    /** */
    private BinaryMetadataTransport transport;

    /** Cached affinity key field names. */
    private final ConcurrentHashMap<Integer, T1<BinaryField>> affKeyFields = new ConcurrentHashMap<>();

    /*
     * Static initializer
     */
    static {
        IMMUTABLE_CLS.add(String.class);
        IMMUTABLE_CLS.add(Boolean.class);
        IMMUTABLE_CLS.add(Byte.class);
        IMMUTABLE_CLS.add(Short.class);
        IMMUTABLE_CLS.add(Character.class);
        IMMUTABLE_CLS.add(Integer.class);
        IMMUTABLE_CLS.add(Long.class);
        IMMUTABLE_CLS.add(Float.class);
        IMMUTABLE_CLS.add(Double.class);
        IMMUTABLE_CLS.add(UUID.class);
        IMMUTABLE_CLS.add(IgniteUuid.class);
        IMMUTABLE_CLS.add(BigDecimal.class);
    }

    /**
     * @param ctx Kernal context.
     */
    @SuppressWarnings("deprecation")
    public CacheObjectBinaryProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        marsh = ctx.grid().configuration().getMarshaller();

        ctx.systemView().registerView(BINARY_METADATA_VIEW, BINARY_METADATA_DESC, new BinaryMetadataViewWalker(),
            metadataLocCache.values(), val -> new BinaryMetadataView(val.metadata()));
    }

    /**
     * @param igniteWorkDir Basic ignite working directory.
     * @param consId Node consistent id.
     * @return Working directory.
     */
    public static File resolveBinaryWorkDir(String igniteWorkDir, String consId) {
        File workDir = binaryWorkDir(igniteWorkDir, consId);

        if (!U.mkdirs(workDir))
            throw new IgniteException("Could not create directory for binary metadata: " + workDir);

        return workDir;
    }

    /**
     * @param igniteWorkDir Basic ignite working directory.
     * @param consId Node consistent id.
     * @return Working directory.
     */
    public static File binaryWorkDir(String igniteWorkDir, String consId) {
        if (F.isEmpty(igniteWorkDir) || F.isEmpty(consId)) {
            throw new IgniteException("Work directory or consistent id has not been set " +
                "[igniteWorkDir=" + igniteWorkDir + ", consId=" + consId + ']');
        }

        return Paths.get(igniteWorkDir, DataStorageConfiguration.DFLT_BINARY_METADATA_PATH, consId).toFile();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (marsh instanceof BinaryMarshaller) {
            if (!ctx.clientNode()) {
                metadataFileStore = new BinaryMetadataFileStore(metadataLocCache,
                    ctx,
                    log,
                    CU.isPersistenceEnabled(ctx.config()) && binaryMetadataFileStoreDir == null ?
                        resolveBinaryWorkDir(ctx.config().getWorkDirectory(),
                            ctx.pdsFolderResolver().resolveFolders().folderName()) :
                        binaryMetadataFileStoreDir);

                metadataFileStore.start();
            }

            BinaryMetadataHandler metaHnd = new BinaryMetadataHandler() {
                @Override public void addMeta(
                    int typeId,
                    BinaryType newMeta,
                    boolean failIfUnregistered) throws BinaryObjectException {
                    assert newMeta != null;
                    assert newMeta instanceof BinaryTypeImpl;

                    if (!discoveryStarted) {
                        BinaryMetadataHolder holder = metadataLocCache.get(typeId);

                        BinaryMetadata oldMeta = holder != null ? holder.metadata() : null;

                        BinaryMetadata mergedMeta = mergeMetadata(oldMeta, ((BinaryTypeImpl)newMeta).metadata());

                        if (oldMeta != mergedMeta)
                            metadataLocCache.put(typeId, new BinaryMetadataHolder(mergedMeta, 0, 0));

                        return;
                    }

                    BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

                    CacheObjectBinaryProcessorImpl.this.addMeta(
                        typeId,
                        newMeta0.wrap(binaryCtx),
                        failIfUnregistered
                    );
                }

                @Override public void addMetaLocally(int typeId, BinaryType meta, boolean failIfUnregistered)
                    throws BinaryObjectException {
                    CacheObjectBinaryProcessorImpl.this.addMetaLocally(typeId, meta);
                }

                @Override public BinaryType metadata(int typeId) throws BinaryObjectException {
                    return CacheObjectBinaryProcessorImpl.this.metadata(typeId);
                }

                @Override public BinaryMetadata metadata0(int typeId) throws BinaryObjectException {
                    return CacheObjectBinaryProcessorImpl.this.metadata0(typeId);
                }

                @Override public BinaryType metadata(int typeId, int schemaId) throws BinaryObjectException {
                    return CacheObjectBinaryProcessorImpl.this.metadata(typeId, schemaId);
                }

                @Override public Collection<BinaryType> metadata() throws BinaryObjectException {
                    return CacheObjectBinaryProcessorImpl.this.metadata();
                }
            };

            BinaryMarshaller bMarsh0 = (BinaryMarshaller)marsh;

            binaryCtx = useTestBinaryCtx ?
                new TestBinaryContext(metaHnd, ctx.config(), ctx.log(BinaryContext.class)) :
                new BinaryContext(metaHnd, ctx.config(), ctx.log(BinaryContext.class));

            transport = new BinaryMetadataTransport(metadataLocCache, metadataFileStore, binaryCtx, ctx, log);

            IgniteUtils.invoke(BinaryMarshaller.class, bMarsh0, "setBinaryContext", binaryCtx, ctx.config());

            binaryMarsh = new GridBinaryMarshaller(binaryCtx);

            binaries = new IgniteBinaryImpl(ctx, this);

            if (!getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
                BinaryConfiguration bCfg = ctx.config().getBinaryConfiguration();

                if (bCfg != null) {
                    Map<String, Object> map = new HashMap<>();

                    map.put("globIdMapper", bCfg.getIdMapper() != null ? bCfg.getIdMapper().getClass().getName() : null);
                    map.put("globSerializer", bCfg.getSerializer() != null ? bCfg.getSerializer().getClass() : null);
                    map.put("compactFooter", bCfg.isCompactFooter());

                    if (bCfg.getTypeConfigurations() != null) {
                        Map<Object, Object> typeCfgsMap = new HashMap<>();

                        for (BinaryTypeConfiguration c : bCfg.getTypeConfigurations()) {
                            typeCfgsMap.put(
                                c.getTypeName() != null,
                                Arrays.asList(
                                    c.getIdMapper() != null ? c.getIdMapper().getClass() : null,
                                    c.getSerializer() != null ? c.getSerializer().getClass() : null,
                                    c.isEnum()
                                )
                            );

                            if (c.isEnum())
                                BinaryUtils.validateEnumValues(c.getTypeName(), c.getEnumValues());
                        }

                        map.put("typeCfgs", typeCfgsMap);
                    }

                    ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_BINARY_CONFIGURATION, map);
                }
            }

            if (!ctx.clientNode())
                metadataFileStore.restoreMetadata();
        }
    }

    /**
     * @param lsnr Listener.
     */
    public void addBinaryMetadataUpdateListener(BinaryMetadataUpdatedListener lsnr) {
        if (transport != null)
            transport.addBinaryMetadataUpdateListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        if (transport != null)
            transport.stop();

        if (metadataFileStore != null)
            metadataFileStore.stop();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        this.reconnectFut = reconnectFut;

        if (transport != null)
            transport.onDisconnected();

        binaryContext().unregisterUserTypeDescriptors();
        binaryContext().unregisterBinarySchemas();

        metadataLocCache.clear();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        reconnectFut = null;

        return super.onReconnected(clusterRestarted);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        discoveryStarted = true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject prepareForCache(@Nullable CacheObject obj, GridCacheContext cctx) {
        if (obj == null)
            return null;

        return obj.prepareForCache(cctx.cacheObjectContext());
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        if (binaryCtx == null)
            return 0;

        return binaryCtx.typeId(typeName);
    }

    /** {@inheritDoc} */
    @Override public boolean immutable(Object obj) {
        assert obj != null;

        return IMMUTABLE_CLS.contains(obj.getClass());
    }

    /** {@inheritDoc} */
    @Override public void onContinuousProcessorStarted(GridKernalContext ctx) {
        // No-op.
    }

    /**
     * @param obj Object.
     * @return Bytes.
     * @throws BinaryObjectException If failed.
     */
    public byte[] marshal(@Nullable Object obj) throws BinaryObjectException {
        byte[] arr = binaryMarsh.marshal(obj, false);

        assert arr.length > 0;

        return arr;
    }

    /**
     * @param ptr Off-heap pointer.
     * @param forceHeap If {@code true} creates heap-based object.
     * @return Object.
     * @throws BinaryObjectException If failed.
     */
    public Object unmarshal(long ptr, boolean forceHeap) throws BinaryObjectException {
        assert ptr > 0 : ptr;

        int size = GridUnsafe.getInt(ptr);

        ptr += 4;

        byte type = GridUnsafe.getByte(ptr++);

        if (type != CacheObject.TYPE_BYTE_ARR) {
            assert size > 0 : size;

            BinaryInputStream in = new BinaryOffheapInputStream(ptr, size, forceHeap);

            return binaryMarsh.unmarshal(in);
        }
        else
            return U.copyMemory(ptr, size);
    }

    /** {@inheritDoc} */
    @Override public Object marshalToBinary(
        @Nullable Object obj,
        boolean failIfUnregistered
    ) throws BinaryObjectException {
        if (obj == null)
            return null;

        if (BinaryUtils.isBinaryType(obj.getClass()))
            return obj;

        if (obj instanceof Object[]) {
            Object[] arr = (Object[])obj;

            Object[] pArr = new Object[arr.length];

            for (int i = 0; i < arr.length; i++)
                pArr[i] = marshalToBinary(arr[i], failIfUnregistered);

            return pArr;
        }

        if (obj instanceof IgniteBiTuple) {
            IgniteBiTuple tup = (IgniteBiTuple)obj;

            if (obj instanceof T2)
                return new T2<>(marshalToBinary(tup.get1(), failIfUnregistered),
                    marshalToBinary(tup.get2(), failIfUnregistered));

            return new IgniteBiTuple<>(marshalToBinary(tup.get1(), failIfUnregistered),
                marshalToBinary(tup.get2(), failIfUnregistered));
        }

        {
            Collection<Object> pCol = BinaryUtils.newKnownCollection(obj);

            if (pCol != null) {
                Collection<?> col = (Collection<?>)obj;

                for (Object item : col)
                    pCol.add(marshalToBinary(item, failIfUnregistered));

                return (pCol instanceof MutableSingletonList) ? U.convertToSingletonList(pCol) : pCol;
            }
        }

        {
            Map<Object, Object> pMap = BinaryUtils.newKnownMap(obj);

            if (pMap != null) {
                Map<?, ?> map = (Map<?, ?>)obj;

                for (Map.Entry<?, ?> e : map.entrySet())
                    pMap.put(marshalToBinary(e.getKey(), failIfUnregistered),
                        marshalToBinary(e.getValue(), failIfUnregistered));

                return pMap;
            }
        }

        if (obj instanceof Map.Entry) {
            Map.Entry<?, ?> e = (Map.Entry<?, ?>)obj;

            return new GridMapEntry<>(marshalToBinary(e.getKey(), failIfUnregistered),
                marshalToBinary(e.getValue(), failIfUnregistered));
        }

        if (binaryMarsh.mustDeserialize(obj))
            return obj; // No need to go through marshal-unmarshal because result will be the same as initial object.

        byte[] arr = binaryMarsh.marshal(obj, failIfUnregistered);

        assert arr.length > 0;

        Object obj0 = binaryMarsh.unmarshal(arr, null);

        // Possible if a class has writeObject method.
        if (obj0 instanceof BinaryObjectImpl)
            ((BinaryObjectImpl)obj0).detachAllowed(true);

        return obj0;
    }

    /**
     * @return Marshaller.
     */
    public GridBinaryMarshaller marshaller() {
        return binaryMarsh;
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(String clsName) {
        return new BinaryObjectBuilderImpl(binaryCtx, clsName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(BinaryObject binaryObj) {
        return BinaryObjectBuilderImpl.wrap(binaryObj);
    }

    /** {@inheritDoc} */
    @Override public void updateMetadata(int typeId, String typeName, @Nullable String affKeyFieldName,
        Map<String, BinaryFieldMetadata> fieldTypeIds, boolean isEnum, @Nullable Map<String, Integer> enumMap)
        throws BinaryObjectException {
        BinaryMetadata meta = new BinaryMetadata(typeId, typeName, fieldTypeIds, affKeyFieldName, null, isEnum,
            enumMap);

        binaryCtx.updateMetadata(typeId, meta, false);
    }

    /** {@inheritDoc} */
    @Override public void addMeta(final int typeId, final BinaryType newMeta, boolean failIfUnregistered)
        throws BinaryObjectException {
        assert newMeta != null;
        assert newMeta instanceof BinaryTypeImpl;

        BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

        if (failIfUnregistered) {
            failIfUnregistered(typeId, newMeta0);

            return;
        }

        try {
            GridFutureAdapter<MetadataUpdateResult> fut = transport.requestMetadataUpdate(newMeta0);

            if (fut == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Metadata update was skipped [typeId=" + typeId
                        + ", typeName=" + newMeta.typeName() + ']');
                }

                return;
            }

            long t0 = System.nanoTime();

            MetadataUpdateResult res = fut.get();

            if (log.isDebugEnabled()) {
                IgniteInternalTx tx = ctx.cache().context().tm().tx();

                log.debug("Completed metadata update [typeId=" + typeId +
                    ", typeName=" + newMeta.typeName() +
                    ", waitTime=" + MILLISECONDS.convert(System.nanoTime() - t0, NANOSECONDS) + "ms" +
                    ", fut=" + fut +
                    ", tx=" + CU.txString(tx) +
                    ']');
            }

            assert res != null;

            if (res.rejected())
                throw res.error();
            else if (!ctx.clientNode())
                metadataFileStore.waitForWriteCompletion(typeId, res.typeVersion());
        }
        catch (IgniteCheckedException e) {
            IgniteCheckedException ex = e;

            if (ctx.isStopping()) {
                ex = new NodeStoppingException("Node is stopping.");

                ex.addSuppressed(e);
            }

            throw new BinaryObjectException("Failed to update metadata for type: " + newMeta.typeName(), ex);
        }
    }

    /**
     * Throw specific exception if given binary metadata is unregistered.
     *
     * @param typeId Type id.
     * @param newMeta0 Expected binary metadata.
     */
    private void failIfUnregistered(int typeId, BinaryMetadata newMeta0) {
        BinaryMetadataHolder metaHolder = metadataLocCache.get(typeId);

        BinaryMetadata oldMeta = metaHolder != null ? metaHolder.metadata() : null;

        BinaryMetadata mergedMeta = mergeMetadata(oldMeta, newMeta0);

        if (mergedMeta != oldMeta)
            throw new UnregisteredBinaryTypeException(typeId, mergedMeta);

        if (metaHolder.pendingVersion() == metaHolder.acceptedVersion())
            return;

        // Metadata locally is up-to-date. Waiting for updating metadata in an entire cluster, if necessary.
        GridFutureAdapter<MetadataUpdateResult> fut = transport.awaitMetadataUpdate(typeId, metaHolder.pendingVersion());

        if (!fut.isDone())
            throw new UnregisteredBinaryTypeException(typeId, fut);
    }

    /** {@inheritDoc} */
    @Override public void addMetaLocally(int typeId, BinaryType newMeta) throws BinaryObjectException {
        assert newMeta != null;
        assert newMeta instanceof BinaryTypeImpl;

        BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

        BinaryMetadataHolder metaHolder = metadataLocCache.get(typeId);

        BinaryMetadata oldMeta = metaHolder != null ? metaHolder.metadata() : null;

        try {
            BinaryMetadata mergedMeta = mergeMetadata(oldMeta, newMeta0);

            if (!ctx.clientNode())
                metadataFileStore.mergeAndWriteMetadata(mergedMeta);

            metadataLocCache.put(typeId, new BinaryMetadataHolder(mergedMeta, 0, 0));
        }
        catch (BinaryObjectException e) {
            throw new BinaryObjectException("New binary metadata is incompatible with binary metadata" +
                " persisted locally." +
                " Consider cleaning up persisted metadata from <workDir>/db/binary_meta directory.", e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public BinaryType metadata(final int typeId) {
        BinaryMetadata meta = metadata0(typeId);

        return meta != null ? meta.wrap(binaryCtx) : null;
    }

    /**
     * Forces caller thread to wait for binary metadata write operation for given type ID.
     *
     * In case of in-memory mode this method becomes a No-op as no binary metadata is written to disk in this mode.
     *
     * @param typeId ID of binary type to wait for metadata write operation.
     */
    public void waitMetadataWriteIfNeeded(final int typeId) {
        if (metadataFileStore == null)
            return;

        BinaryMetadataHolder hldr = metadataLocCache.get(typeId);

        if (hldr != null) {
            try {
                metadataFileStore.waitForWriteCompletion(typeId, hldr.pendingVersion());
            }
            catch (IgniteCheckedException e) {
                log.warning("Failed to wait for metadata write operation for [typeId=" + typeId +
                    ", typeVer=" + hldr.acceptedVersion() + ']', e);
            }
        }
    }

    /**
     * @param typeId Type ID.
     * @return Metadata.
     * @throws IgniteException In case of error.
     */
    @Nullable public BinaryMetadata metadata0(final int typeId) {
        BinaryMetadataHolder holder = metadataLocCache.get(typeId);

        IgniteThread curThread = IgniteThread.current();

        if (holder == null && (curThread == null || !curThread.isForbiddenToRequestBinaryMetadata())) {
            if (ctx.clientNode()) {
                try {
                    transport.requestUpToDateMetadata(typeId).get();

                    holder = metadataLocCache.get(typeId);
                }
                catch (IgniteCheckedException ignored) {
                    // No-op.
                }
            }
        }

        if (holder != null) {
            if (holder.removing()) {
                GridFutureAdapter<MetadataUpdateResult> fut = transport.awaitMetadataRemove(typeId);

                try {
                    fut.get();
                }
                catch (IgniteCheckedException ignored) {
                    // No-op.
                }

                return null;
            }

            if (curThread instanceof IgniteDiscoveryThread || (curThread != null && curThread.isForbiddenToRequestBinaryMetadata()))
                return holder.metadata();

            if (holder.pendingVersion() - holder.acceptedVersion() > 0) {
                GridFutureAdapter<MetadataUpdateResult> fut = transport.awaitMetadataUpdate(typeId, holder.pendingVersion());

                if (log.isDebugEnabled() && !fut.isDone())
                    log.debug("Waiting for update for" +
                        " [typeId=" + typeId +
                        ", pendingVer=" + holder.pendingVersion() +
                        ", acceptedVer=" + holder.acceptedVersion() + "]");

                try {
                    fut.get();
                }
                catch (IgniteCheckedException ignored) {
                    // No-op.
                }
            }
            else if (metadataFileStore != null) {
                try {
                    metadataFileStore.waitForWriteCompletion(typeId, holder.pendingVersion());
                }
                catch (IgniteCheckedException e) {
                    log.warning("Failed to wait for metadata write operation for [typeId=" + typeId +
                        ", typeVer=" + holder.acceptedVersion() + ']', e);

                    return null;
                }
            }

            return holder.metadata();
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public BinaryType metadata(final int typeId, final int schemaId) {
        BinaryMetadataHolder holder = metadataLocCache.get(typeId);

        if (ctx.clientNode()) {
            if (holder == null || !holder.metadata().hasSchema(schemaId)) {
                if (log.isDebugEnabled())
                    log.debug("Waiting for client metadata update" +
                        " [typeId=" + typeId
                        + ", schemaId=" + schemaId
                        + ", pendingVer=" + (holder == null ? "NA" : holder.pendingVersion())
                        + ", acceptedVer=" + (holder == null ? "NA" : holder.acceptedVersion()) + ']');

                try {
                    transport.requestUpToDateMetadata(typeId).get();
                }
                catch (IgniteCheckedException ignored) {
                    // No-op.
                }

                holder = metadataLocCache.get(typeId);

                IgniteFuture<?> reconnectFut0 = reconnectFut;

                if (holder == null && reconnectFut0 != null)
                    throw new IgniteClientDisconnectedException(reconnectFut0, "Client node disconnected.");

                if (log.isDebugEnabled())
                    log.debug("Finished waiting for client metadata update" +
                        " [typeId=" + typeId
                        + ", schemaId=" + schemaId
                        + ", pendingVer=" + (holder == null ? "NA" : holder.pendingVersion())
                        + ", acceptedVer=" + (holder == null ? "NA" : holder.acceptedVersion()) + ']');
            }
        }
        else {
            if (holder != null && IgniteThread.current() instanceof IgniteDiscoveryThread)
                return holder.metadata().wrap(binaryCtx);
            else if (holder != null && (holder.pendingVersion() - holder.acceptedVersion() > 0)) {
                if (log.isDebugEnabled())
                    log.debug("Waiting for metadata update" +
                        " [typeId=" + typeId
                        + ", schemaId=" + schemaId
                        + ", pendingVer=" + holder.pendingVersion()
                        + ", acceptedVer=" + holder.acceptedVersion() + ']');

                long t0 = System.nanoTime();

                GridFutureAdapter<MetadataUpdateResult> fut = transport.awaitMetadataUpdate(
                    typeId,
                    holder.pendingVersion());

                try {
                    fut.get();
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to wait for metadata update [typeId=" + typeId + ", schemaId=" + schemaId + ']', e);
                }

                if (log.isDebugEnabled())
                    log.debug("Finished waiting for metadata update" +
                        " [typeId=" + typeId
                        + ", waitTime=" + NANOSECONDS.convert(System.nanoTime() - t0, MILLISECONDS) + "ms"
                        + ", schemaId=" + schemaId
                        + ", pendingVer=" + holder.pendingVersion()
                        + ", acceptedVer=" + holder.acceptedVersion() + ']');

                holder = metadataLocCache.get(typeId);
            }
            else if (holder == null || !holder.metadata().hasSchema(schemaId)) {
                // Last resort waiting.
                U.warn(log,
                    "Schema is missing while no metadata updates are in progress " +
                        "(will wait for schema update within timeout defined by " + IGNITE_WAIT_SCHEMA_UPDATE + " system property)" +
                        " [typeId=" + typeId
                        + ", missingSchemaId=" + schemaId
                        + ", pendingVer=" + (holder == null ? "NA" : holder.pendingVersion())
                        + ", acceptedVer=" + (holder == null ? "NA" : holder.acceptedVersion())
                        + ", binMetaUpdateTimeout=" + waitSchemaTimeout + ']');

                long t0 = System.nanoTime();

                GridFutureAdapter<?> fut = transport.awaitSchemaUpdate(typeId, schemaId);

                try {
                    fut.get(waitSchemaTimeout);
                }
                catch (IgniteFutureTimeoutCheckedException e) {
                    log.error("Timed out while waiting for schema update [typeId=" + typeId + ", schemaId=" +
                        schemaId + ']');
                }
                catch (IgniteCheckedException ignored) {
                    // No-op.
                }

                holder = metadataLocCache.get(typeId);

                if (log.isDebugEnabled() && holder != null && holder.metadata().hasSchema(schemaId))
                    log.debug("Found the schema after wait" +
                        " [typeId=" + typeId
                        + ", waitTime=" + NANOSECONDS.convert(System.nanoTime() - t0, MILLISECONDS) + "ms"
                        + ", schemaId=" + schemaId
                        + ", pendingVer=" + holder.pendingVersion()
                        + ", acceptedVer=" + holder.acceptedVersion() + ']');
            }
        }

        if (holder != null && metadataFileStore != null) {
            try {
                metadataFileStore.waitForWriteCompletion(typeId, holder.pendingVersion());
            }
            catch (IgniteCheckedException e) {
                log.warning("Failed to wait for metadata write operation for [typeId=" + typeId +
                    ", typeVer=" + holder.acceptedVersion() + ']', e);

                return null;
            }
        }

        return holder != null ? holder.metadata().wrap(binaryCtx) : null;
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, BinaryType> metadata(Collection<Integer> typeIds)
        throws BinaryObjectException {
        try {
            Map<Integer, BinaryType> res = U.newHashMap(metadataLocCache.size());

            for (Map.Entry<Integer, BinaryMetadataHolder> e : metadataLocCache.entrySet())
                res.put(e.getKey(), e.getValue().metadata().wrap(binaryCtx));

            return res;
        }
        catch (CacheException e) {
            throw new BinaryObjectException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryType> metadata() throws BinaryObjectException {
        return F.viewReadOnly(metadataLocCache.values(), new IgniteClosure<BinaryMetadataHolder, BinaryType>() {
            @Override public BinaryType apply(BinaryMetadataHolder metaHolder) {
                return metaHolder.metadata().wrap(binaryCtx);
            }
        });
    }

    /**
     * @return Cluster binary metadata.
     * @throws BinaryObjectException on error.
     */
    public Collection<BinaryMetadata> binaryMetadata() throws BinaryObjectException {
        return F.viewReadOnly(metadataLocCache.values(), new IgniteClosure<BinaryMetadataHolder, BinaryMetadata>() {
            @Override public BinaryMetadata apply(BinaryMetadataHolder metaHolder) {
                return metaHolder.metadata();
            }
        });
    }

    /**
     * @return Binary metadata for specified type.
     * @throws BinaryObjectException on error.
     */
    public BinaryMetadata binaryMetadata(int typeId) throws BinaryObjectException {
        BinaryMetadataHolder hld = metadataLocCache.get(typeId);

        return hld != null ? hld.metadata() : null;
    }

    /** {@inheritDoc} */
    @Override public void saveMetadata(Collection<BinaryType> types, File dir) {
        try {
            BinaryMetadataFileStore writer = new BinaryMetadataFileStore(new ConcurrentHashMap<>(),
                ctx,
                log,
                resolveBinaryWorkDir(dir.getAbsolutePath(),
                    ctx.pdsFolderResolver().resolveFolders().folderName()));

            for (BinaryType type : types)
                writer.mergeAndWriteMetadata(((BinaryTypeImpl)type).metadata());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, int ord) throws BinaryObjectException {
        A.notNullOrEmpty(typeName, "enum type name");

        int typeId = binaryCtx.typeId(typeName);

        typeName = binaryCtx.userTypeName(typeName);

        updateMetadata(typeId, typeName, null, null, true, null);

        return new BinaryEnumObjectImpl(binaryCtx, typeId, null, ord);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, String name) throws BinaryObjectException {
        A.notNullOrEmpty(typeName, "enum type name");
        A.notNullOrEmpty(name, "enum name");

        int typeId = binaryCtx.typeId(typeName);

        BinaryMetadata metadata = metadata0(typeId);

        if (metadata == null)
            throw new BinaryObjectException("Failed to get metadata for type [typeId=" +
                    typeId + ", typeName='" + typeName + "']");

        Integer ordinal = metadata.getEnumOrdinalByName(name);

        typeName = binaryCtx.userTypeName(typeName);

        if (ordinal == null)
            throw new BinaryObjectException("Failed to resolve enum ordinal by name [typeId=" +
                    typeId + ", typeName='" + typeName + "', name='" + name + "']");

        return new BinaryEnumObjectImpl(binaryCtx, typeId, null, ordinal);
    }

    /** {@inheritDoc} */
    @Override public BinaryType registerEnum(String typeName, Map<String, Integer> vals) throws BinaryObjectException {
        A.notNullOrEmpty(typeName, "enum type name");

        int typeId = binaryCtx.typeId(typeName);

        typeName = binaryCtx.userTypeName(typeName);

        BinaryUtils.validateEnumValues(typeName, vals);

        updateMetadata(typeId, typeName, null, null, true, vals);

        return binaryCtx.metadata(typeId);
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() throws IgniteException {
        return binaries;
    }

    /** {@inheritDoc} */
    @Override public boolean isBinaryObject(Object obj) {
        return obj instanceof BinaryObject;
    }

    /** {@inheritDoc} */
    @Override public boolean isBinaryEnabled(CacheConfiguration<?, ?> ccfg) {
        return marsh instanceof BinaryMarshaller;
    }

    /**
     * Get affinity key field.
     *
     * @param typeId Binary object type ID.
     * @return Affinity key.
     */
    public BinaryField affinityKeyField(int typeId) {
        // Fast path for already cached field.
        T1<BinaryField> fieldHolder = affKeyFields.get(typeId);

        if (fieldHolder != null)
            return fieldHolder.get();

        // Slow path if affinity field is not cached yet.
        String name = binaryCtx.affinityKeyFieldName(typeId);

        if (name != null) {
            BinaryField field = binaryCtx.createField(typeId, name);

            affKeyFields.putIfAbsent(typeId, new T1<>(field));

            return field;
        }
        else {
            affKeyFields.putIfAbsent(typeId, new T1<>(null));

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public int typeId(Object obj) {
        if (obj == null)
            return 0;

        return isBinaryObject(obj) ? ((BinaryObjectEx)obj).typeId() : typeId(obj.getClass().getSimpleName());
    }

    /** {@inheritDoc} */
    @Override public Object field(Object obj, String fieldName) {
        if (obj == null)
            return null;

        return isBinaryObject(obj) ? ((BinaryObject)obj).field(fieldName) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(Object obj, String fieldName) {
        return obj != null && ((BinaryObject)obj).hasField(fieldName);
    }

    /**
     * @return Binary context.
     */
    public BinaryContext binaryContext() {
        return binaryCtx;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public CacheObjectContext contextForCache(CacheConfiguration ccfg) throws IgniteCheckedException {
        assert ccfg != null;

        boolean storeVal = !ccfg.isCopyOnRead() || (!isBinaryEnabled(ccfg) &&
            (QueryUtils.isEnabled(ccfg) || ctx.config().isPeerClassLoadingEnabled()));

        boolean binaryEnabled = marsh instanceof BinaryMarshaller && !GridCacheUtils.isSystemCache(ccfg.getName());

        AffinityKeyMapper cacheAffMapper = ccfg.getAffinityMapper();

        AffinityKeyMapper dfltAffMapper = binaryEnabled ?
            new CacheDefaultBinaryAffinityKeyMapper(ccfg.getKeyConfiguration()) :
            new GridCacheDefaultAffinityKeyMapper();

        ctx.resource().injectGeneric(dfltAffMapper);

        return new CacheObjectContext(ctx,
            ccfg.getName(),
            dfltAffMapper,
            QueryUtils.isCustomAffinityMapper(ccfg.getAffinityMapper()),
            ccfg.isCopyOnRead(),
            storeVal,
            ctx.config().isPeerClassLoadingEnabled() && !isBinaryEnabled(ccfg),
            binaryEnabled
        );
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(CacheObjectValueContext ctx, Object val) throws IgniteCheckedException {
        if (!ctx.binaryEnabled() || binaryMarsh == null)
            return CU.marshal(ctx.kernalContext().cache().context(), ctx.addDeploymentInfo(), val);

        byte[] arr = binaryMarsh.marshal(val, false);

        assert arr.length > 0;

        return arr;
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(CacheObjectValueContext ctx, byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException {
        if (!ctx.binaryEnabled() || binaryMarsh == null)
            return U.unmarshal(ctx.kernalContext(), bytes, U.resolveClassLoader(clsLdr, ctx.kernalContext().config()));

        return binaryMarsh.unmarshal(bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject toCacheKeyObject(CacheObjectContext ctx, @Nullable GridCacheContext cctx,
        Object obj, boolean userObj) {
        if (!ctx.binaryEnabled()) {
            if (obj instanceof KeyCacheObject) {
                KeyCacheObject key = (KeyCacheObject)obj;

                if (key.partition() == -1)
                    // Assume all KeyCacheObjects except BinaryObject can not be reused for another cache.
                    key.partition(partition(ctx, cctx, key));

                return (KeyCacheObject)obj;
            }

            return toCacheKeyObject0(ctx, cctx, obj, userObj);
        }

        if (obj instanceof KeyCacheObject) {
            KeyCacheObject key = (KeyCacheObject)obj;

            if (key instanceof BinaryObjectImpl) {
                // Need to create a copy because the key can be reused at the application layer after that (IGNITE-3505).
                key = key.copy(partition(ctx, cctx, key));
            }
            else if (key.partition() == -1)
                // Assume others KeyCacheObjects can not be reused for another cache.
                key.partition(partition(ctx, cctx, key));

            return key;
        }

        obj = toBinary(obj, false);

        if (obj instanceof BinaryObjectImpl) {
            ((KeyCacheObject) obj).partition(partition(ctx, cctx, obj));

            return (KeyCacheObject)obj;
        }

        return toCacheKeyObject0(ctx, cctx, obj, userObj);
    }

    /**
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Key cache object.
     */
    protected KeyCacheObject toCacheKeyObject0(CacheObjectContext ctx,
        @Nullable GridCacheContext cctx,
        Object obj,
        boolean userObj) {
        int part = partition(ctx, cctx, obj);

        if (!userObj)
            return new KeyCacheObjectImpl(obj, null, part);

        return new UserKeyCacheObjectImpl(obj, part);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj,
        boolean userObj, boolean failIfUnregistered) {
        if (!ctx.binaryEnabled()) {
            if (obj == null || obj instanceof CacheObject)
                return (CacheObject)obj;

            return toCacheObject0(obj, userObj);
        }

        if (obj == null || obj instanceof CacheObject)
            return (CacheObject)obj;

        obj = toBinary(obj, failIfUnregistered);

        if (obj instanceof CacheObject)
            return (CacheObject)obj;

        return toCacheObject0(obj, userObj);
    }

    /**
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Cache object.
     */
    private CacheObject toCacheObject0(@Nullable Object obj, boolean userObj) {
        assert obj != null;

        if (obj instanceof byte[]) {
            if (!userObj)
                return new CacheObjectByteArrayImpl((byte[])obj);

            return new UserCacheObjectByteArrayImpl((byte[])obj);
        }

        if (!userObj)
            return new CacheObjectImpl(obj, null);

        return new UserCacheObjectImpl(obj, null);
    }

    /**
     * @param ctx Cache objects context.
     * @param cctx Cache context.
     * @param obj Object.
     * @return Object partition.
     */
    private int partition(CacheObjectContext ctx, @Nullable GridCacheContext cctx, Object obj) {
        try {
            return cctx != null ?
                cctx.affinity().partition(obj, false) :
                ctx.kernalContext().affinity().partition0(ctx.cacheName(), obj, null);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to get partition", e);

            return -1;
        }
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(CacheObjectContext ctx, byte type, byte[] bytes) {
        switch (type) {
            case BinaryObjectImpl.TYPE_BINARY:
                return new BinaryObjectImpl(binaryContext(), bytes, 0);

            case BinaryObjectImpl.TYPE_BINARY_ENUM:
                return new BinaryEnumObjectImpl(binaryContext(), bytes);

            case CacheObject.TYPE_BYTE_ARR:
                return new CacheObjectByteArrayImpl(bytes);

            case CacheObject.TYPE_REGULAR:
                return new CacheObjectImpl(null, bytes);
        }

        throw new IllegalArgumentException("Invalid object type: " + type);
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject toKeyCacheObject(CacheObjectContext ctx, byte type, byte[] bytes)
        throws IgniteCheckedException {
        switch (type) {
            case BinaryObjectImpl.TYPE_BINARY:
                return new BinaryObjectImpl(binaryContext(), bytes, 0);

            case CacheObject.TYPE_BYTE_ARR:
                throw new IllegalArgumentException("Byte arrays cannot be used as cache keys.");

            case CacheObject.TYPE_REGULAR:
                return new KeyCacheObjectImpl(ctx.kernalContext().cacheObjects().unmarshal(ctx, bytes, null), bytes, -1);
        }

        throw new IllegalArgumentException("Invalid object type: " + type);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(CacheObjectContext ctx, ByteBuffer buf) {
        int len = buf.getInt();

        assert len >= 0 : len;

        byte type = buf.get();

        byte[] data = new byte[len];

        buf.get(data);

        return toCacheObject(ctx, type, data);
    }

    /** {@inheritDoc} */
    @Override public IncompleteCacheObject toCacheObject(CacheObjectContext ctx, ByteBuffer buf,
        @Nullable IncompleteCacheObject incompleteObj) {
        if (incompleteObj == null)
            incompleteObj = new IncompleteCacheObject(buf);

        if (incompleteObj.isReady())
            return incompleteObj;

        incompleteObj.readData(buf);

        if (incompleteObj.isReady())
            incompleteObj.object(toCacheObject(ctx, incompleteObj.type(), incompleteObj.data()));

        return incompleteObj;
    }

    /** {@inheritDoc} */
    @Override public IncompleteCacheObject toKeyCacheObject(CacheObjectContext ctx, ByteBuffer buf,
        @Nullable IncompleteCacheObject incompleteObj) throws IgniteCheckedException {
        if (incompleteObj == null)
            incompleteObj = new IncompleteCacheObject(buf);

        if (incompleteObj.isReady())
            return incompleteObj;

        incompleteObj.readData(buf);

        if (incompleteObj.isReady())
            incompleteObj.object(toKeyCacheObject(ctx, incompleteObj.type(), incompleteObj.data()));

        return incompleteObj;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj,
        boolean userObj) {
        return toCacheObject(ctx, obj, userObj, false);
    }

    /** {@inheritDoc} */
    @Override public Object unwrapTemporary(GridCacheContext ctx, Object obj) throws BinaryObjectException {
        if (!ctx.cacheObjectContext().binaryEnabled())
            return obj;

        if (obj instanceof BinaryObjectOffheapImpl)
            return ((BinaryObjectOffheapImpl)obj).heapCopy();

        return obj;
    }

    /**
     * @param obj Object.
     * @return Binary object.
     * @throws IgniteException In case of error.
     */
    @Nullable public Object toBinary(@Nullable Object obj, boolean failIfUnregistered) throws IgniteException {
        if (obj == null)
            return null;

        if (isBinaryObject(obj))
            return obj;

        return marshalToBinary(obj, failIfUnregistered);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode rmtNode,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData
    ) {
        IgniteNodeValidationResult res;

        if (getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK) || !(marsh instanceof BinaryMarshaller))
            return null;

        if ((res = validateBinaryConfiguration(rmtNode)) != null)
            return res;

        return validateBinaryMetadata(rmtNode.id(), (Map<Integer, BinaryMetadataHolder>)discoData.joiningNodeData());
    }

    /** */
    private IgniteNodeValidationResult validateBinaryConfiguration(ClusterNode rmtNode) {
        Object rmtBinaryCfg = rmtNode.attribute(IgniteNodeAttributes.ATTR_BINARY_CONFIGURATION);

        ClusterNode locNode = ctx.discovery().localNode();

        Object locBinaryCfg = locNode.attribute(IgniteNodeAttributes.ATTR_BINARY_CONFIGURATION);

        if (!F.eq(locBinaryCfg, rmtBinaryCfg)) {
            String msg = "Local node's binary configuration is not equal to remote node's binary configuration " +
                "[locNodeId=%s, rmtNodeId=%s, locBinaryCfg=%s, rmtBinaryCfg=%s]";

            return new IgniteNodeValidationResult(rmtNode.id(),
                String.format(msg, locNode.id(), rmtNode.id(), locBinaryCfg, rmtBinaryCfg),
                String.format(msg, rmtNode.id(), locNode.id(), rmtBinaryCfg, locBinaryCfg));
        }

        return null;
    }

    /** */
    private IgniteNodeValidationResult validateBinaryMetadata(UUID rmtNodeId, Map<Integer, BinaryMetadataHolder> newNodeMeta) {
        if (newNodeMeta == null)
            return null;

        for (Map.Entry<Integer, BinaryMetadataHolder> metaEntry : newNodeMeta.entrySet()) {
            if (!metadataLocCache.containsKey(metaEntry.getKey()))
                continue;

            BinaryMetadata locMeta = metadataLocCache.get(metaEntry.getKey()).metadata();
            BinaryMetadata rmtMeta = metaEntry.getValue().metadata();

            if (locMeta == null || rmtMeta == null)
                continue;

            try {
                mergeMetadata(locMeta, rmtMeta);
            }
            catch (Exception e) {
                String locMsg = "Exception was thrown when merging binary metadata from node %s: %s";

                String rmtMsg = "Exception was thrown on coordinator when merging binary metadata from this node: %s";

                return new IgniteNodeValidationResult(rmtNodeId,
                    String.format(locMsg, rmtNodeId.toString(), e.getMessage()),
                    String.format(rmtMsg, e.getMessage()));
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return BINARY_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (!dataBag.commonDataCollectedFor(BINARY_PROC.ordinal())) {
            Map<Integer, BinaryMetadataHolder> res = U.newHashMap(metadataLocCache.size());

            for (Map.Entry<Integer,BinaryMetadataHolder> e : metadataLocCache.entrySet()) {
                if (!e.getValue().removing())
                    res.put(e.getKey(), e.getValue());
            }

            dataBag.addGridCommonData(BINARY_PROC.ordinal(), (Serializable) res);
        }
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        Map<Integer, BinaryMetadataHolder> res = U.newHashMap(metadataLocCache.size());

        for (Map.Entry<Integer,BinaryMetadataHolder> e : metadataLocCache.entrySet())
            res.put(e.getKey(), e.getValue());

        dataBag.addJoiningNodeData(BINARY_PROC.ordinal(), (Serializable) res);
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        Map<Integer,BinaryMetadataHolder> newNodeMeta = (Map<Integer, BinaryMetadataHolder>) data.joiningNodeData();

        if (newNodeMeta == null)
            return;

        UUID joiningNode = data.joiningNodeId();

        for (Map.Entry<Integer, BinaryMetadataHolder> metaEntry : newNodeMeta.entrySet()) {
            if (metadataLocCache.containsKey(metaEntry.getKey())) {
                BinaryMetadataHolder localMetaHolder = metadataLocCache.get(metaEntry.getKey());

                BinaryMetadata newMeta = metaEntry.getValue().metadata();
                BinaryMetadata localMeta = localMetaHolder.metadata();

                BinaryMetadata mergedMeta = mergeMetadata(localMeta, newMeta);

                if (mergedMeta != localMeta) {
                    //put mergedMeta to local cache and store to disk
                    U.log(log,
                        String.format("Newer version of existing BinaryMetadata[typeId=%d, typeName=%s] " +
                                "is received from node %s; updating it locally",
                            mergedMeta.typeId(),
                            mergedMeta.typeName(),
                            joiningNode));

                    metadataLocCache.put(metaEntry.getKey(),
                        new BinaryMetadataHolder(mergedMeta,
                            localMetaHolder.pendingVersion(),
                            localMetaHolder.acceptedVersion()));

                    if (!ctx.clientNode())
                        metadataFileStore.writeMetadata(mergedMeta);
                }
            }
            else {
                BinaryMetadataHolder newMetaHolder = metaEntry.getValue();
                BinaryMetadata newMeta = newMetaHolder.metadata();

                U.log(log,
                    String.format("New BinaryMetadata[typeId=%d, typeName=%s] " +
                            "is received from node %s; adding it locally",
                        newMeta.typeId(),
                        newMeta.typeName(),
                        joiningNode));

                metadataLocCache.put(metaEntry.getKey(), newMetaHolder);

                if (!ctx.clientNode())
                    metadataFileStore.writeMetadata(newMeta);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        Map<Integer, BinaryMetadataHolder> receivedData = (Map<Integer, BinaryMetadataHolder>) data.commonData();

        if (receivedData != null) {
            for (Map.Entry<Integer, BinaryMetadataHolder> e : receivedData.entrySet()) {
                BinaryMetadataHolder holder = e.getValue();

                BinaryMetadataHolder localHolder = new BinaryMetadataHolder(holder.metadata(),
                        holder.pendingVersion(),
                        holder.pendingVersion());

                if (log.isDebugEnabled())
                    log.debug("Received metadata on join: " + localHolder);

                metadataLocCache.put(e.getKey(), localHolder);

                if (!ctx.clientNode())
                    metadataFileStore.writeMetadata(holder.metadata());
            }
        }
    }

    /**
     * Sets path to binary metadata store configured by user, should include binary_meta and consistentId
     * @param binaryMetadataFileStoreDir path to binary_meta
     */
    public void setBinaryMetadataFileStoreDir(@Nullable File binaryMetadataFileStoreDir) {
        this.binaryMetadataFileStoreDir = binaryMetadataFileStoreDir;
    }

    /** {@inheritDoc} */
    @Override public void removeType(int typeId) {
        BinaryMetadataHolder oldHld = metadataLocCache.get(typeId);

        if (oldHld == null)
            throw new IgniteException("Failed to remove metadata, type not found: " + typeId);

        if (oldHld.removing())
            throw new IgniteException("Failed to remove metadata, type is being removed: " + typeId);

        if (!IgniteFeatures.allNodesSupports(ctx.discovery().allNodes(), IgniteFeatures.REMOVE_METADATA)) {
            throw new IgniteException("Failed to remove metadata, " +
                "all cluster nodes must support the remove type feature");
        }

        try {
            GridFutureAdapter<MetadataUpdateResult> fut = transport.requestMetadataRemove(typeId);

            MetadataUpdateResult res = fut.get();

            if (res.rejected())
                throw res.error();
        }
        catch (IgniteCheckedException e) {
            IgniteCheckedException ex = e;

            if (ctx.isStopping()) {
                ex = new NodeStoppingException("Node is stopping.");

                ex.addSuppressed(e);
            }

            throw new BinaryObjectException("Failed to remove metadata for type: " + typeId, ex);
        }
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class TestBinaryContext extends BinaryContext {
        /** */
        private List<TestBinaryContextListener> listeners;

        /**
         * @param metaHnd Meta handler.
         * @param igniteCfg Ignite config.
         * @param log Logger.
         */
        public TestBinaryContext(BinaryMetadataHandler metaHnd, IgniteConfiguration igniteCfg,
            IgniteLogger log) {
            super(metaHnd, igniteCfg, log);
        }

        /** {@inheritDoc} */
        @Nullable @Override public BinaryType metadata(int typeId) throws BinaryObjectException {
            BinaryType metadata = super.metadata(typeId);

            if (listeners != null) {
                for (TestBinaryContextListener listener : listeners)
                    listener.onAfterMetadataRequest(typeId, metadata);
            }

            return metadata;
        }

        /** {@inheritDoc} */
        @Override public void updateMetadata(int typeId, BinaryMetadata meta,
            boolean failIfUnregistered) throws BinaryObjectException {
            if (listeners != null) {
                for (TestBinaryContextListener listener : listeners)
                    listener.onBeforeMetadataUpdate(typeId, meta);
            }

            super.updateMetadata(typeId, meta, failIfUnregistered);
        }

        /** */
        public interface TestBinaryContextListener {
            /**
             * @param typeId Type id.
             * @param type Type.
             */
            void onAfterMetadataRequest(int typeId, BinaryType type);

            /**
             * @param typeId Type id.
             * @param metadata Metadata.
             */
            void onBeforeMetadataUpdate(int typeId, BinaryMetadata metadata);
        }

        /**
         * @param lsnr Listener.
         */
        public void addListener(TestBinaryContextListener lsnr) {
            if (listeners == null)
                listeners = new ArrayList<>();

            if (!listeners.contains(lsnr))
                listeners.add(lsnr);
        }

        /** */
        public void clearAllListener() {
            if (listeners != null)
                listeners.clear();
        }
    }
}
