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

package org.apache.ignite.internal.client.thin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientAtomicConfiguration;
import org.apache.ignite.client.ClientAtomicLong;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientCluster;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientCollectionConfiguration;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.client.ClientServices;
import org.apache.ignite.client.ClientTransactions;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.events.ClientFailEvent;
import org.apache.ignite.client.events.ClientLifecycleEventListener;
import org.apache.ignite.client.events.ClientStartEvent;
import org.apache.ignite.client.events.ClientStopEvent;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientTransactionConfiguration;
import org.apache.ignite.internal.MarshallerPlatformIds;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.client.thin.TcpClientTransactions.TcpClientTransaction;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link IgniteClient} over TCP protocol.
 */
public class TcpIgniteClient implements IgniteClient {
    /** Channel. */
    private final ReliableChannel ch;

    /** Ignite Binary. */
    private final IgniteBinary binary;

    /** Transactions facade. */
    private final TcpClientTransactions transactions;

    /** Compute facade. */
    private final ClientComputeImpl compute;

    /** Cluster facade. */
    private final ClientClusterImpl cluster;

    /** Services facade. */
    private final ClientServicesImpl services;

    /** Registered entry listeners for all caches. */
    private final ClientCacheEntryListenersRegistry lsnrsRegistry;

    /** Event listeners. */
    private final EventListener[] evtLsnrs;

    /** Marshaller. */
    private final ClientBinaryMarshaller marsh;

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Private constructor. Use {@link TcpIgniteClient#start(ClientConfiguration)} to create an instance of
     * {@code TcpIgniteClient}.
     */
    private TcpIgniteClient(ClientConfiguration cfg) throws ClientException {
        this(TcpClientChannel::new, cfg);
    }

    /**
     * Constructor with custom channel factory.
     */
    TcpIgniteClient(
            BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory,
            ClientConfiguration cfg
    ) throws ClientException {
        log = NullLogger.whenNull(cfg.getLogger());

        final ClientBinaryMetadataHandler metadataHnd = new ClientBinaryMetadataHandler();

        ClientMarshallerContext marshCtx = new ClientMarshallerContext();
        marsh = new ClientBinaryMarshaller(metadataHnd, marshCtx);

        marsh.setBinaryConfiguration(cfg.getBinaryConfiguration());

        serDes = new ClientUtils(marsh);

        binary = new ClientBinary(marsh);

        ch = new ReliableChannel(chFactory, cfg, binary);

        evtLsnrs = cfg.getEventListeners() == null ? null : cfg.getEventListeners().clone();

        try {
            ch.channelsInit();

            retrieveBinaryConfiguration(cfg);

            // Metadata, binary descriptors and user types caches must be cleared so that the
            // client will register all the user types within the cluster once again in case this information
            // was lost during the cluster failover.
            ch.addChannelFailListener(() -> {
                metadataHnd.onReconnect();
                marshCtx.clearUserTypesCache();
                marsh.context().unregisterUserTypeDescriptors();
            });

            // Send postponed metadata after channel init.
            metadataHnd.sendAllMeta();

            transactions = new TcpClientTransactions(ch, marsh,
                    new ClientTransactionConfiguration(cfg.getTransactionConfiguration()));

            cluster = new ClientClusterImpl(ch, marsh);

            compute = new ClientComputeImpl(ch, marsh, cluster.defaultClusterGroup());

            services = new ClientServicesImpl(ch, marsh, cluster.defaultClusterGroup(), log);

            lsnrsRegistry = new ClientCacheEntryListenersRegistry();
        }
        catch (Exception e) {
            ch.close();
            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ch.close();

        ClientStopEvent evt = new ClientStopEvent(this);

        triggerLifecycleEventListeners(log, evtLsnrs, lsnr -> lsnr.onClientStop(evt));
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> getOrCreateCache(String name) throws ClientException {
        ensureCacheName(name);

        ch.request(ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME, req -> writeString(name, req.out()));

        return new TcpClientCache<>(name, ch, marsh, transactions, lsnrsRegistry);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteClientFuture<ClientCache<K, V>> getOrCreateCacheAsync(String name) throws ClientException {
        ensureCacheName(name);

        return new IgniteClientFutureImpl<>(
                ch.requestAsync(ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME, req -> writeString(name, req.out()))
                        .thenApply(x -> new TcpClientCache<>(name, ch, marsh, transactions, lsnrsRegistry)));
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> getOrCreateCache(
        ClientCacheConfiguration cfg) throws ClientException {
        ensureCacheConfiguration(cfg);

        ch.request(ClientOperation.CACHE_GET_OR_CREATE_WITH_CONFIGURATION,
            req -> serDes.cacheConfiguration(cfg, req.out(), req.clientChannel().protocolCtx()));

        return new TcpClientCache<>(cfg.getName(), ch, marsh, transactions, lsnrsRegistry);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteClientFuture<ClientCache<K, V>> getOrCreateCacheAsync(
            ClientCacheConfiguration cfg) throws ClientException {
        ensureCacheConfiguration(cfg);

        return new IgniteClientFutureImpl<>(
                ch.requestAsync(ClientOperation.CACHE_GET_OR_CREATE_WITH_CONFIGURATION,
                        req -> serDes.cacheConfiguration(cfg, req.out(), req.clientChannel().protocolCtx()))
                        .thenApply(x -> new TcpClientCache<>(cfg.getName(), ch, marsh, transactions, lsnrsRegistry)));
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> cache(String name) {
        ensureCacheName(name);

        return new TcpClientCache<>(name, ch, marsh, transactions, lsnrsRegistry);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() throws ClientException {
        return ch.service(ClientOperation.CACHE_GET_NAMES,
                res -> Arrays.asList(BinaryUtils.doReadStringArray(res.in())));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Collection<String>> cacheNamesAsync() throws ClientException {
        return ch.serviceAsync(ClientOperation.CACHE_GET_NAMES,
                res -> Arrays.asList(BinaryUtils.doReadStringArray(res.in())));
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String name) throws ClientException {
        ensureCacheName(name);

        ch.request(ClientOperation.CACHE_DESTROY, req -> req.out().writeInt(ClientUtils.cacheId(name)));
        ch.unregisterCacheIfCustomAffinity(name);
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> destroyCacheAsync(String name) throws ClientException {
        ensureCacheName(name);

        return ch.requestAsync(ClientOperation.CACHE_DESTROY, req -> {
            req.out().writeInt(ClientUtils.cacheId(name));
            ch.unregisterCacheIfCustomAffinity(name);
        });
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> createCache(String name) throws ClientException {
        ensureCacheName(name);

        ch.request(ClientOperation.CACHE_CREATE_WITH_NAME, req -> writeString(name, req.out()));

        return new TcpClientCache<>(name, ch, marsh, transactions, lsnrsRegistry);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteClientFuture<ClientCache<K, V>> createCacheAsync(String name) throws ClientException {
        ensureCacheName(name);

        return new IgniteClientFutureImpl<>(
                ch.requestAsync(ClientOperation.CACHE_CREATE_WITH_NAME, req -> writeString(name, req.out()))
                        .thenApply(x -> new TcpClientCache<>(name, ch, marsh, transactions, lsnrsRegistry)));
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> createCache(ClientCacheConfiguration cfg) throws ClientException {
        ensureCacheConfiguration(cfg);

        ch.request(ClientOperation.CACHE_CREATE_WITH_CONFIGURATION,
            req -> serDes.cacheConfiguration(cfg, req.out(), req.clientChannel().protocolCtx()));

        return new TcpClientCache<>(cfg.getName(), ch, marsh, transactions, lsnrsRegistry);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteClientFuture<ClientCache<K, V>> createCacheAsync(ClientCacheConfiguration cfg)
            throws ClientException {
        ensureCacheConfiguration(cfg);

        return new IgniteClientFutureImpl<>(
                ch.requestAsync(ClientOperation.CACHE_CREATE_WITH_CONFIGURATION,
                        req -> serDes.cacheConfiguration(cfg, req.out(), req.clientChannel().protocolCtx()))
                        .thenApply(x -> new TcpClientCache<>(cfg.getName(), ch, marsh, transactions, lsnrsRegistry)));
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        return binary;
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        if (qry == null)
            throw new NullPointerException("qry");

        Consumer<PayloadOutputChannel> qryWriter = payloadCh -> {
            BinaryOutputStream out = payloadCh.out();

            byte flags = TcpClientCache.KEEP_BINARY_FLAG_MASK;

            int txId = 0;

            if (payloadCh.clientChannel().protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.TX_AWARE_QUERIES)) {
                TcpClientTransaction tx = transactions.tx();

                txId = tx == null ? 0 : tx.txId();
            }

            out.writeInt(0); // no cache ID

            if (txId != 0) {
                flags |= TcpClientCache.TRANSACTIONAL_FLAG_MASK;

                out.writeByte(flags);
                out.writeInt(txId);
            }
            else
                out.writeByte(flags);

            serDes.write(qry, out);
        };

        return new ClientFieldsQueryCursor<>(new ClientFieldsQueryPager(
            ch,
            transactions.tx(),
            ClientOperation.QUERY_SQL_FIELDS,
            ClientOperation.QUERY_SQL_FIELDS_CURSOR_GET_PAGE,
            qryWriter,
            true,
            marsh
        ));
    }

    /** {@inheritDoc} */
    @Override public ClientTransactions transactions() {
        return transactions;
    }

    /** {@inheritDoc} */
    @Override public ClientCompute compute() {
        return compute;
    }

    /** {@inheritDoc} */
    @Override public ClientCompute compute(ClientClusterGroup grp) {
        return compute.withClusterGroup((ClientClusterGroupImpl)grp);
    }

    /** {@inheritDoc} */
    @Override public ClientCluster cluster() {
        return cluster;
    }

    /** {@inheritDoc} */
    @Override public ClientServices services() {
        return services;
    }

    /** {@inheritDoc} */
    @Override public ClientServices services(ClientClusterGroup grp) {
        return services.withClusterGroup((ClientClusterGroupImpl)grp);
    }

    /** {@inheritDoc} */
    @Override public ClientAtomicLong atomicLong(String name, long initVal, boolean create) {
        return atomicLong(name, null, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public ClientAtomicLong atomicLong(String name, ClientAtomicConfiguration cfg, long initVal, boolean create) {
        GridArgumentCheck.notNull(name, "name");

        if (create) {
            ch.service(ClientOperation.ATOMIC_LONG_CREATE, out -> {
                writeString(name, out.out());
                out.out().writeLong(initVal);

                if (cfg != null) {
                    out.out().writeBoolean(true);
                    out.out().writeInt(cfg.getAtomicSequenceReserveSize());
                    out.out().writeByte(CacheMode.toCode(cfg.getCacheMode()));
                    out.out().writeInt(cfg.getBackups());
                    writeString(cfg.getGroupName(), out.out());
                }
                else
                    out.out().writeBoolean(false);
            }, null);
        }

        ClientAtomicLong res = new ClientAtomicLongImpl(name, cfg != null ? cfg.getGroupName() : null, ch);

        // Return null when specified atomic long does not exist to match IgniteKernal behavior.
        if (!create && res.removed())
            return null;

        return res;
    }

    /** {@inheritDoc} */
    @Override public <T> ClientIgniteSet<T> set(String name, @Nullable ClientCollectionConfiguration cfg) {
        GridArgumentCheck.notNull(name, "name");

        return ch.service(ClientOperation.OP_SET_GET_OR_CREATE, out -> {
            writeString(name, out.out());

            if (cfg != null) {
                out.out().writeBoolean(true);
                out.out().writeByte((byte)cfg.getAtomicityMode().ordinal());
                out.out().writeByte(CacheMode.toCode(cfg.getCacheMode()));
                out.out().writeInt(cfg.getBackups());
                writeString(cfg.getGroupName(), out.out());
                out.out().writeBoolean(cfg.isColocated());
            }
            else
                out.out().writeBoolean(false);
        }, in -> {
            if (!in.in().readBoolean())
                return null;

            boolean colocated = in.in().readBoolean();
            int cacheId = in.in().readInt();

            return new ClientIgniteSetImpl<>(ch, serDes, name, colocated, cacheId);
        });
    }

    /**
     * Initializes new instance of {@link IgniteClient}.
     *
     * @param cfg Thin client configuration.
     * @return Client with successfully opened thin client connection.
     */
    public static IgniteClient start(ClientConfiguration cfg) throws ClientException {
        try {
            TcpIgniteClient client = new TcpIgniteClient(cfg);

            ClientStartEvent evt = new ClientStartEvent(client, cfg);

            triggerLifecycleEventListeners(client.log, client.evtLsnrs, lsnr -> lsnr.onClientStart(evt));

            return client;
        }
        catch (Throwable throwable) {
            ClientFailEvent evt = new ClientFailEvent(cfg, throwable);

            triggerLifecycleEventListeners(cfg.getLogger(), cfg.getEventListeners(), lsnr -> lsnr.onClientFail(evt));

            throw throwable;
        }
    }

    /** */
    private static void triggerLifecycleEventListeners(
        @Nullable IgniteLogger log,
        EventListener[] lsnrs,
        Consumer<ClientLifecycleEventListener> action
    ) {
        if (F.isEmpty(lsnrs))
            return;

        for (EventListener lsnr: lsnrs) {
            if (lsnr instanceof ClientLifecycleEventListener) {
                try {
                    ClientLifecycleEventListener lsnr0 = (ClientLifecycleEventListener)lsnr;

                    action.accept(lsnr0);
                }
                catch (Exception e) {
                    if (log != null)
                        log.warning("Exception thrown while consuming event in listener " + lsnr, e);
                }
            }
        }
    }

    /**
     * @return Channel.
     */
    ReliableChannel reliableChannel() {
        return ch;
    }

    /** @throws IllegalArgumentException if the specified cache name is invalid. */
    private static void ensureCacheName(String name) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("Cache name must be specified");
    }

    /** @throws IllegalArgumentException if the specified cache name is invalid. */
    private static void ensureCacheConfiguration(ClientCacheConfiguration cfg) {
        if (cfg == null)
            throw new IllegalArgumentException("Cache configuration must be specified");

        ensureCacheName(cfg.getName());
    }

    /** Serialize string. */
    private void writeString(String s, BinaryOutputStream out) {
        try (BinaryRawWriterEx w = new BinaryWriterExImpl(marsh.context(), out, null, null)) {
            w.writeString(s);
        }
    }

    /** Deserialize string. */
    private String readString(BinaryInputStream in) throws BinaryObjectException {
        try {
            try (BinaryReaderExImpl r = serDes.createBinaryReader(in)) {
                return r.readString();
            }
        }
        catch (IOException e) {
            throw new BinaryObjectException(e);
        }
    }

    /** Load cluster binary configration. */
    private void retrieveBinaryConfiguration(ClientConfiguration cfg) {
        if (!cfg.isAutoBinaryConfigurationEnabled())
            return;

        ClientInternalBinaryConfiguration clusterCfg = ch.applyOnDefaultChannel(
                c -> c.protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.BINARY_CONFIGURATION)
                ? c.service(ClientOperation.GET_BINARY_CONFIGURATION, null, r -> new ClientInternalBinaryConfiguration(r.in()))
                : null,
                ClientOperation.GET_BINARY_CONFIGURATION);

        if (clusterCfg == null)
            return;

        if (log.isDebugEnabled())
            log.debug("Cluster binary configuration retrieved: " + clusterCfg);

        BinaryConfiguration resCfg = cfg.getBinaryConfiguration();

        if (resCfg == null)
            resCfg = new BinaryConfiguration();

        if (resCfg.isCompactFooter() != clusterCfg.compactFooter()) {
            if (log.isInfoEnabled())
                log.info("Overriding compact footer setting according to cluster configuration: " +
                        resCfg.isCompactFooter() + " -> " + clusterCfg.compactFooter());

            resCfg.setCompactFooter(clusterCfg.compactFooter());
        }

        switch (clusterCfg.binaryNameMapperMode()) {
            case BASIC_FULL:
                resCfg.setNameMapper(new BinaryBasicNameMapper().setSimpleName(false));
                break;

            case BASIC_SIMPLE:
                resCfg.setNameMapper(new BinaryBasicNameMapper().setSimpleName(true));
                break;

            case CUSTOM:
                if (resCfg.getNameMapper() == null || resCfg.getNameMapper() instanceof BinaryBasicNameMapper) {
                    throw new IgniteClientException(ClientStatus.FAILED,
                            "Custom binary name mapper is configured on the server, but not on the client. "
                                    + "Update client BinaryConfigration to match the server.");
                }

                break;
        }

        marsh.setBinaryConfiguration(resCfg);
    }

    /**
     * Thin client implementation of {@link BinaryMetadataHandler}.
     */
    private class ClientBinaryMetadataHandler implements BinaryMetadataHandler {
        /** In-memory metadata cache. */
        private volatile BinaryMetadataHandler cache = BinaryCachingMetadataHandler.create();

        /** {@inheritDoc} */
        @Override public void addMeta(int typeId, BinaryType meta, boolean failIfUnregistered)
            throws BinaryObjectException {
            BinaryType oldType = cache.metadata(typeId);
            BinaryMetadata oldMeta = oldType == null ? null : ((BinaryTypeImpl)oldType).metadata();
            BinaryMetadata newMeta = ((BinaryTypeImpl)meta).metadata();

            // If type wasn't registered before or metadata changed, send registration request.
            if (oldType == null || BinaryUtils.mergeMetadata(oldMeta, newMeta) != oldMeta) {
                try {
                    if (ch != null) { // Postpone binary type registration requests to server before channels initiated.
                        ch.request(
                            ClientOperation.PUT_BINARY_TYPE,
                            req -> serDes.binaryMetadata(newMeta, req.out())
                        );
                    }
                }
                catch (ClientException e) {
                    throw new BinaryObjectException(e);
                }
            }

            cache.addMeta(typeId, meta, failIfUnregistered); // merge
        }

        /** Send registration requests to the server for all collected metadata. */
        public void sendAllMeta() {
            try {
                CompletableFuture.allOf(cache.metadata().stream()
                    .map(type -> sendMetaAsync(((BinaryTypeImpl)type).metadata()).toCompletableFuture())
                    .toArray(CompletableFuture[]::new)
                ).get();
            }
            catch (Exception e) {
                throw new ClientException(e);
            }
        }

        /** Send metadata registration request to the server. */
        private IgniteClientFuture<Void> sendMetaAsync(BinaryMetadata meta) {
            return ch.requestAsync(ClientOperation.PUT_BINARY_TYPE, req -> serDes.binaryMetadata(meta, req.out()));
        }

        /** {@inheritDoc} */
        @Override public void addMetaLocally(int typeId, BinaryType meta, boolean failIfUnregistered)
            throws BinaryObjectException {
            throw new UnsupportedOperationException("Can't register metadata locally for thin client.");
        }

        /** {@inheritDoc} */
        @Override public BinaryType metadata(int typeId) throws BinaryObjectException {
            BinaryType meta = cache.metadata(typeId);

            if (meta == null)
                meta = requestAndCacheBinaryType(typeId);

            return meta;
        }

        /** {@inheritDoc} */
        @Override public BinaryMetadata metadata0(int typeId) throws BinaryObjectException {
            BinaryMetadata meta = cache.metadata0(typeId);

            if (meta == null)
                meta = requestBinaryMetadata(typeId);

            return meta;
        }

        /** {@inheritDoc} */
        @Override public BinaryType metadata(int typeId, int schemaId) throws BinaryObjectException {
            BinaryType meta = cache.metadata(typeId);

            if (hasSchema(meta, schemaId))
                return meta;

            meta = requestAndCacheBinaryType(typeId);

            return hasSchema(meta, schemaId) ? meta : null;
        }

        /** {@inheritDoc} */
        @Override public Collection<BinaryType> metadata() throws BinaryObjectException {
            return cache.metadata();
        }

        /**
         * @param type Binary type.
         * @param schemaId Schema id.
         */
        private boolean hasSchema(BinaryType type, int schemaId) {
            return type != null && ((BinaryTypeImpl)type).metadata().hasSchema(schemaId);
        }

        /**
         * Request binary metadata from server and add binary type to cache.
         *
         * @param typeId Type id.
         */
        private BinaryType requestAndCacheBinaryType(int typeId) throws BinaryObjectException {
            BinaryMetadata meta0 = requestBinaryMetadata(typeId);

            if (meta0 == null)
                return null;

            BinaryType meta = new BinaryTypeImpl(marsh.context(), meta0);

            cache.addMeta(typeId, meta, false);

            return meta;
        }

        /**
         * Request binary metadata for type id from the server.
         *
         * @param typeId Type id.
         */
        private BinaryMetadata requestBinaryMetadata(int typeId) throws BinaryObjectException {
            try {
                return ch.service(
                    ClientOperation.GET_BINARY_TYPE,
                    req -> req.out().writeInt(typeId),
                    res -> {
                        try {
                            return res.in().readBoolean() ? serDes.binaryMetadata(res.in()) : null;
                        }
                        catch (IOException e) {
                            throw new BinaryObjectException(e);
                        }
                    }
                );
            }
            catch (ClientException e) {
                throw new BinaryObjectException(e);
            }
        }

        /**
         * Clear local cache on reconnect.
         */
        void onReconnect() {
            cache = BinaryCachingMetadataHandler.create();
        }
    }

    /**
     * Thin client implementation of {@link MarshallerContext}.
     */
    private class ClientMarshallerContext implements MarshallerContext {
        /** Type ID -> class name map. */
        private final Map<Integer, String> cache = new ConcurrentHashMap<>();

        /** System types. */
        private final Collection<String> sysTypes = new HashSet<>();

        /**
         * Default constructor.
         */
        public ClientMarshallerContext() {
            try {
                MarshallerUtils.processSystemClasses(U.gridClassLoader(), null, sysTypes::add);
            }
            catch (IOException e) {
                throw new IllegalStateException("Failed to initialize marshaller context.", e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean registerClassName(
            byte platformId,
            int typeId,
            String clsName,
            boolean failIfUnregistered
        ) throws IgniteCheckedException {

            if (platformId != MarshallerPlatformIds.JAVA_ID)
                throw new IllegalArgumentException("platformId");

            boolean res = true;

            if (!cache.containsKey(typeId)) {
                try {
                    res = ch.service(
                        ClientOperation.REGISTER_BINARY_TYPE_NAME,
                        payloadCh -> {
                            BinaryOutputStream out = payloadCh.out();

                            out.writeByte(platformId);
                            out.writeInt(typeId);
                            writeString(clsName, out);
                        },
                        payloadCh -> payloadCh.in().readBoolean()
                    );
                }
                catch (ClientException e) {
                    throw new IgniteCheckedException(e);
                }

                if (res)
                    cache.put(typeId, clsName);
            }

            return res;
        }

        /** {@inheritDoc} */
        @Deprecated
        @Override public boolean registerClassName(byte platformId, int typeId, String clsName) throws IgniteCheckedException {
            return registerClassName(platformId, typeId, clsName, false);
        }

        /** {@inheritDoc} */
        @Override public boolean registerClassNameLocally(byte platformId, int typeId, String clsName) {
            if (platformId != MarshallerPlatformIds.JAVA_ID)
                throw new IllegalArgumentException("platformId");

            cache.put(typeId, clsName);

            return true;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("rawtypes")
        @Override public Class getClass(int typeId, ClassLoader ldr)
            throws ClassNotFoundException, IgniteCheckedException {

            return U.forName(getClassName(MarshallerPlatformIds.JAVA_ID, typeId), ldr, null);
        }

        /** {@inheritDoc} */
        @Override public String getClassName(byte platformId, int typeId)
            throws ClassNotFoundException, IgniteCheckedException {

            if (platformId != MarshallerPlatformIds.JAVA_ID)
                throw new IllegalArgumentException("platformId");

            String clsName = cache.get(typeId);

            if (clsName == null) {
                try {
                    clsName = ch.service(
                        ClientOperation.GET_BINARY_TYPE_NAME,
                        req -> {
                            BinaryOutputStream out = req.out();

                            out.writeByte(platformId);
                            out.writeInt(typeId);
                        },
                        res -> readString(res.in())
                    );
                }
                catch (ClientException e) {
                    throw new IgniteCheckedException(e);
                }

                if (clsName != null)
                    cache.putIfAbsent(typeId, clsName);
            }

            if (clsName == null)
                throw new ClassNotFoundException(String.format("Unknown type id [%s]", typeId));

            return clsName;
        }

        /** {@inheritDoc} */
        @Override public boolean isSystemType(String typeName) {
            return sysTypes.contains(typeName);
        }

        /** {@inheritDoc} */
        @Override public IgnitePredicate<String> classNameFilter() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public JdkMarshaller jdkMarshaller() {
            return new JdkMarshaller();
        }

        /**
         * Clear the user types cache on reconnect so that the client will register all
         * the types once again after reconnect.
         * See the comment in constructor of the TcpIgniteClient.
         */
        void clearUserTypesCache() {
            cache.clear();
        }
    }
}
