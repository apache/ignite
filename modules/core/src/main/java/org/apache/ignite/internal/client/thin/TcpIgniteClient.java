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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

/**
 * Implementation of {@link IgniteClient} over TCP protocol.
 */
public class TcpIgniteClient implements IgniteClient {
    /** Channel. */
    private final ReliableChannel ch;

    /** Ignite Binary. */
    private final IgniteBinary binary;

    /** Marshaller. */
    private final ClientBinaryMarshaller marsh;

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    /**
     * Private constructor. Use {@link TcpIgniteClient#start(ClientConfiguration)} to create an instance of
     * {@link TcpClientChannel}.
     */
    private TcpIgniteClient(ClientConfiguration cfg) throws ClientException {
        Function<ClientChannelConfiguration, Result<ClientChannel>> chFactory = chCfg -> {
            try {
                return new Result<>(new TcpClientChannel(chCfg));
            }
            catch (ClientException e) {
                return new Result<>(e);
            }
        };

        ch = new ReliableChannel(chFactory, cfg);

        marsh = new ClientBinaryMarshaller(new ClientBinaryMetadataHandler(), new ClientMarshallerContext());

        marsh.setBinaryConfiguration(cfg.getBinaryConfiguration());

        serDes = new ClientUtils(marsh);

        binary = new ClientBinary(marsh);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        ch.close();
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> getOrCreateCache(String name) throws ClientException {
        ensureCacheName(name);

        ch.request(ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME, req -> writeString(name, req));

        return new TcpClientCache<>(name, ch, marsh);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> getOrCreateCache(
        ClientCacheConfiguration cfg) throws ClientException {
        ensureCacheConfiguration(cfg);

        ch.request(ClientOperation.CACHE_GET_OR_CREATE_WITH_CONFIGURATION, req -> serDes.cacheConfiguration(cfg, req));

        return new TcpClientCache<>(cfg.getName(), ch, marsh);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> cache(String name) {
        ensureCacheName(name);

        return new TcpClientCache<>(name, ch, marsh);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() throws ClientException {
        return ch.service(ClientOperation.CACHE_GET_NAMES, res -> Arrays.asList(BinaryUtils.doReadStringArray(res)));
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String name) throws ClientException {
        ensureCacheName(name);

        ch.request(ClientOperation.CACHE_DESTROY, req -> req.writeInt(ClientUtils.cacheId(name)));
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> createCache(String name) throws ClientException {
        ensureCacheName(name);

        ch.request(ClientOperation.CACHE_CREATE_WITH_NAME, req -> writeString(name, req));

        return new TcpClientCache<>(name, ch, marsh);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> createCache(ClientCacheConfiguration cfg) throws ClientException {
        ensureCacheConfiguration(cfg);

        ch.request(ClientOperation.CACHE_CREATE_WITH_CONFIGURATION, req -> serDes.cacheConfiguration(cfg, req));

        return new TcpClientCache<>(cfg.getName(), ch, marsh);
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        return binary;
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        if (qry == null)
            throw new NullPointerException("qry");

        Consumer<BinaryOutputStream> qryWriter = out -> {
            out.writeInt(0); // no cache ID
            out.writeByte((byte)1); // keep binary
            serDes.write(qry, out);
        };

        return new ClientFieldsQueryCursor<>(new ClientFieldsQueryPager(
            ch,
            ClientOperation.QUERY_SQL_FIELDS,
            ClientOperation.QUERY_SQL_FIELDS_CURSOR_GET_PAGE,
            qryWriter,
            true,
            marsh
        ));
    }

    /**
     * Initializes new instance of {@link IgniteClient}.
     * <p>
     * Server connection will be lazily initialized when first required.
     *
     * @param cfg Thin client configuration.
     * @return Successfully opened thin client connection.
     */
    public static IgniteClient start(ClientConfiguration cfg) throws ClientException {
        return new TcpIgniteClient(cfg);
    }

    /** @throws IllegalArgumentException if the specified cache name is invalid. */
    private static void ensureCacheName(String name) {
        if (name == null || name.length() == 0)
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
            try (BinaryReaderExImpl r = new BinaryReaderExImpl(marsh.context(), in, null, true)) {
                return r.readString();
            }
        }
        catch (IOException e) {
            throw new BinaryObjectException(e);
        }
    }

    /**
     * Thin client implementation of {@link BinaryMetadataHandler}.
     */
    private class ClientBinaryMetadataHandler implements BinaryMetadataHandler {
        /** In-memory metadata cache. */
        private final BinaryMetadataHandler cache = BinaryCachingMetadataHandler.create();

        /** {@inheritDoc} */
        @Override public void addMeta(int typeId, BinaryType meta) throws BinaryObjectException {
            if (cache.metadata(typeId) == null) {
                try {
                    ch.request(
                        ClientOperation.PUT_BINARY_TYPE,
                        req -> serDes.binaryMetadata(((BinaryTypeImpl)meta).metadata(), req)
                    );
                }
                catch (ClientException e) {
                    throw new BinaryObjectException(e);
                }
            }

            cache.addMeta(typeId, meta); // merge
        }

        /** {@inheritDoc} */
        @Override public BinaryType metadata(int typeId) throws BinaryObjectException {
            BinaryType meta = cache.metadata(typeId);

            if (meta == null) {
                BinaryMetadata meta0 = metadata0(typeId);

                if (meta0 != null) {
                    meta = new BinaryTypeImpl(marsh.context(), meta0);

                    cache.addMeta(typeId, meta);
                }
            }

            return meta;
        }

        /** {@inheritDoc} */
        @Override public BinaryMetadata metadata0(int typeId) throws BinaryObjectException {
            BinaryMetadata meta = cache.metadata0(typeId);

            if (meta == null) {
                try {
                    meta = ch.service(
                        ClientOperation.GET_BINARY_TYPE,
                        req -> req.writeInt(typeId),
                        res -> {
                            try {
                                return res.readBoolean() ? serDes.binaryMetadata(res) : null;
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

            return meta;
        }

        /** {@inheritDoc} */
        @Override public BinaryType metadata(int typeId, int schemaId) throws BinaryObjectException {
            BinaryType meta = metadata(typeId);

            return meta != null && ((BinaryTypeImpl)meta).metadata().hasSchema(schemaId) ? meta : null;
        }

        /** {@inheritDoc} */
        @Override public Collection<BinaryType> metadata() throws BinaryObjectException {
            return cache.metadata();
        }
    }

    /**
     * Thin client implementation of {@link MarshallerContext}.
     */
    private class ClientMarshallerContext implements MarshallerContext {
        /** Type ID -> class name map. */
        private Map<Integer, String> cache = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public boolean registerClassName(byte platformId, int typeId, String clsName)
            throws IgniteCheckedException {

            if (platformId != MarshallerPlatformIds.JAVA_ID)
                throw new IllegalArgumentException("platformId");

            boolean res = true;

            if (!cache.containsKey(typeId)) {
                try {
                    res = ch.service(
                        ClientOperation.REGISTER_BINARY_TYPE_NAME,
                        req -> {
                            req.writeByte(platformId);
                            req.writeInt(typeId);
                            writeString(clsName, req);
                        },
                        BinaryInputStream::readBoolean
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
        @Override public boolean registerClassNameLocally(byte platformId, int typeId, String clsName) {
            if (platformId != MarshallerPlatformIds.JAVA_ID)
                throw new IllegalArgumentException("platformId");

            cache.put(typeId, clsName);

            return true;
        }

        /** {@inheritDoc} */
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
                            req.writeByte(platformId);
                            req.writeInt(typeId);
                        },
                        TcpIgniteClient.this::readString
                    );
                }
                catch (ClientException e) {
                    throw new IgniteCheckedException(e);
                }
            }

            if (clsName == null)
                throw new ClassNotFoundException(String.format("Unknown type id [%s]", typeId));

            return clsName;
        }

        /** {@inheritDoc} */
        @Override public boolean isSystemType(String typeName) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public IgnitePredicate<String> classNameFilter() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public JdkMarshaller jdkMarshaller() {
            return new JdkMarshaller();
        }
    }
}
