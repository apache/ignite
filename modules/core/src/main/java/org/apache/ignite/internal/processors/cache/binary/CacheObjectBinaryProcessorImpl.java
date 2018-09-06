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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.cache.CacheException;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
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
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessorImpl;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.BINARY_PROC;

/**
 * Binary processor implementation.
 */
public class CacheObjectBinaryProcessorImpl extends IgniteCacheObjectProcessorImpl implements
    CacheObjectBinaryProcessor {
    /** */
    private volatile boolean discoveryStarted;

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

    /** */
    @GridToStringExclude
    private IgniteBinary binaries;

    /** Listener removes all registered binary schemas and user type descriptors after the local client reconnected. */
    private final GridLocalEventListener clientDisconLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            binaryContext().unregisterUserTypeDescriptors();
            binaryContext().unregisterBinarySchemas();

            metadataLocCache.clear();
        }
    };

    /** Locally cached metadata. This local cache is managed by exchanging discovery custom events. */
    private final ConcurrentMap<Integer, BinaryMetadataHolder> metadataLocCache = new ConcurrentHashMap<>();

    /** */
    private BinaryMetadataTransport transport;

    /** Cached affinity key field names. */
    private final ConcurrentHashMap<Integer, T1<BinaryField>> affKeyFields = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public CacheObjectBinaryProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        marsh = ctx.grid().configuration().getMarshaller();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (marsh instanceof BinaryMarshaller) {
            if (ctx.clientNode())
                ctx.event().addLocalEventListener(clientDisconLsnr, EVT_CLIENT_NODE_DISCONNECTED);

            if (!ctx.clientNode())
                metadataFileStore = new BinaryMetadataFileStore(metadataLocCache, ctx, log, binaryMetadataFileStoreDir);

            transport = new BinaryMetadataTransport(metadataLocCache, metadataFileStore, ctx, log);

            BinaryMetadataHandler metaHnd = new BinaryMetadataHandler() {
                @Override public void addMeta(int typeId, BinaryType newMeta, boolean failIfUnregistered) throws BinaryObjectException {
                    assert newMeta != null;
                    assert newMeta instanceof BinaryTypeImpl;

                    if (!discoveryStarted) {
                        BinaryMetadataHolder holder = metadataLocCache.get(typeId);

                        BinaryMetadata oldMeta = holder != null ? holder.metadata() : null;

                        BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(oldMeta, ((BinaryTypeImpl)newMeta).metadata());

                        if (oldMeta != mergedMeta)
                            metadataLocCache.put(typeId, new BinaryMetadataHolder(mergedMeta, 0, 0));

                        return;
                    }

                    BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

                    CacheObjectBinaryProcessorImpl.this.addMeta(typeId, newMeta0.wrap(binaryCtx), failIfUnregistered);
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

            binaryCtx = new BinaryContext(metaHnd, ctx.config(), ctx.log(BinaryContext.class));

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
        if (ctx.clientNode())
            ctx.event().removeLocalEventListener(clientDisconLsnr);

        if (transport != null)
            transport.stop();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        if (transport != null)
            transport.onDisconnected();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        discoveryStarted = true;
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        if (binaryCtx == null)
            return super.typeId(typeName);

        return binaryCtx.typeId(typeName);
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
    @SuppressWarnings("unchecked")
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
    @Override public void addMeta(final int typeId, final BinaryType newMeta, boolean failIfUnregistered) throws BinaryObjectException {
        assert newMeta != null;
        assert newMeta instanceof BinaryTypeImpl;

        BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

        try {
            BinaryMetadataHolder metaHolder = metadataLocCache.get(typeId);

            BinaryMetadata oldMeta = metaHolder != null ? metaHolder.metadata() : null;

            BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(oldMeta, newMeta0);

            //metadata requested to be added is exactly the same as already presented in the cache
            if (mergedMeta == oldMeta)
                return;

            if (failIfUnregistered)
                throw new UnregisteredBinaryTypeException(
                    "Attempted to update binary metadata inside a critical synchronization block (will be " +
                        "automatically retried). This exception must not be wrapped to any other exception class. " +
                        "If you encounter this exception outside of EntryProcessor, please report to Apache Ignite " +
                        "dev-list.",
                    typeId, mergedMeta);

            MetadataUpdateResult res = transport.requestMetadataUpdate(mergedMeta).get();

            assert res != null;

            if (res.rejected())
                throw res.error();
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException("Failed to update meta data for type: " + newMeta.typeName(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void addMetaLocally(int typeId, BinaryType newMeta) throws BinaryObjectException {
        assert newMeta != null;
        assert newMeta instanceof BinaryTypeImpl;

        BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

        BinaryMetadataHolder metaHolder = metadataLocCache.get(typeId);

        BinaryMetadata oldMeta = metaHolder != null ? metaHolder.metadata() : null;

        try {
            BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(oldMeta, newMeta0);

            if (!ctx.clientNode())
                metadataFileStore.mergeAndWriteMetadata(mergedMeta);

            metadataLocCache.put(typeId, new BinaryMetadataHolder(mergedMeta, 0, 0));
        }
        catch (BinaryObjectException e) {
            throw new BinaryObjectException("New binary metadata is incompatible with binary metadata" +
                " persisted locally." +
                " Consider cleaning up persisted metadata from <workDir>/binary_meta directory.", e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public BinaryType metadata(final int typeId) {
        BinaryMetadata meta = metadata0(typeId);

        return meta != null ? meta.wrap(binaryCtx) : null;
    }

    /**
     * @param typeId Type ID.
     * @return Meta data.
     * @throws IgniteException In case of error.
     */
    @Nullable public BinaryMetadata metadata0(final int typeId) {
        BinaryMetadataHolder holder = metadataLocCache.get(typeId);

        if (holder == null) {
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
            if (IgniteThread.current() instanceof IgniteDiscoveryThread)
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
                try {
                    transport.requestUpToDateMetadata(typeId).get();

                    holder = metadataLocCache.get(typeId);
                }
                catch (IgniteCheckedException ignored) {
                    // No-op.
                }
            }
        }
        else if (holder != null) {
            if (IgniteThread.current() instanceof IgniteDiscoveryThread)
                return holder.metadata().wrap(binaryCtx);

            if (holder.pendingVersion() - holder.acceptedVersion() > 0) {
                GridFutureAdapter<MetadataUpdateResult> fut = transport.awaitMetadataUpdate(
                        typeId,
                        holder.pendingVersion());

                if (log.isDebugEnabled() && !fut.isDone())
                    log.debug("Waiting for update for" +
                            " [typeId=" + typeId
                            + ", schemaId=" + schemaId
                            + ", pendingVer=" + holder.pendingVersion()
                            + ", acceptedVer=" + holder.acceptedVersion() + "]");

                try {
                    fut.get();
                }
                catch (IgniteCheckedException ignored) {
                    // No-op.
                }

                holder = metadataLocCache.get(typeId);
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
    @SuppressWarnings("unchecked")
    @Override public Collection<BinaryType> metadata() throws BinaryObjectException {
        return F.viewReadOnly(metadataLocCache.values(), new IgniteClosure<BinaryMetadataHolder, BinaryType>() {
            @Override public BinaryType apply(BinaryMetadataHolder metaHolder) {
                return metaHolder.metadata().wrap(binaryCtx);
            }
        });
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
            affKeyFields.putIfAbsent(typeId, new T1<BinaryField>(null));

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

        return isBinaryObject(obj) ? ((BinaryObject)obj).field(fieldName) : super.field(obj, fieldName);
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
    @Override public CacheObjectContext contextForCache(CacheConfiguration cfg) throws IgniteCheckedException {
        assert cfg != null;

        boolean binaryEnabled = marsh instanceof BinaryMarshaller && !GridCacheUtils.isSystemCache(cfg.getName()) &&
            !GridCacheUtils.isIgfsCache(ctx.config(), cfg.getName());

        CacheObjectContext ctx0 = super.contextForCache(cfg);

        CacheObjectContext res = new CacheObjectBinaryContext(ctx,
            cfg,
            ctx0.copyOnGet(),
            ctx0.storeValue(),
            binaryEnabled,
            ctx0.addDeploymentInfo());

        ctx.resource().injectGeneric(res.defaultAffMapper());

        return res;
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(CacheObjectValueContext ctx, Object val) throws IgniteCheckedException {
        if (!ctx.binaryEnabled() || binaryMarsh == null)
            return super.marshal(ctx, val);

        byte[] arr = binaryMarsh.marshal(val, false);

        assert arr.length > 0;

        return arr;
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(CacheObjectValueContext ctx, byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException {
        if (!ctx.binaryEnabled() || binaryMarsh == null)
            return super.unmarshal(ctx, bytes, clsLdr);

        return binaryMarsh.unmarshal(bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject toCacheKeyObject(CacheObjectContext ctx, @Nullable GridCacheContext cctx,
        Object obj, boolean userObj) {
        if (!ctx.binaryEnabled())
            return super.toCacheKeyObject(ctx, cctx, obj, userObj);

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
            ((BinaryObjectImpl)obj).partition(partition(ctx, cctx, obj));

            return (KeyCacheObject)obj;
        }

        return toCacheKeyObject0(ctx, cctx, obj, userObj);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj,
        boolean userObj, boolean failIfUnregistered) {
        if (!ctx.binaryEnabled())
            return super.toCacheObject(ctx, obj, userObj, failIfUnregistered);

        if (obj == null || obj instanceof CacheObject)
            return (CacheObject)obj;

        obj = toBinary(obj, failIfUnregistered);

        if (obj instanceof CacheObject)
            return (CacheObject)obj;

        return toCacheObject0(obj, userObj);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(CacheObjectContext ctx, byte type, byte[] bytes) {
        if (type == BinaryObjectImpl.TYPE_BINARY)
            return new BinaryObjectImpl(binaryContext(), bytes, 0);
        else if (type == BinaryObjectImpl.TYPE_BINARY_ENUM)
            return new BinaryEnumObjectImpl(binaryContext(), bytes);

        return super.toCacheObject(ctx, type, bytes);
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject toKeyCacheObject(CacheObjectContext ctx, byte type, byte[] bytes)
        throws IgniteCheckedException {
        if (type == BinaryObjectImpl.TYPE_BINARY)
            return new BinaryObjectImpl(binaryContext(), bytes, 0);

        return super.toKeyCacheObject(ctx, type, bytes);
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

        return validateBinaryMetadata(rmtNode.id(), (Map<Integer, BinaryMetadataHolder>) discoData.joiningNodeData());
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
                BinaryUtils.mergeMetadata(locMeta, rmtMeta);
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

            for (Map.Entry<Integer,BinaryMetadataHolder> e : metadataLocCache.entrySet())
                res.put(e.getKey(), e.getValue());

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

                BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(localMeta, newMeta);

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
}
