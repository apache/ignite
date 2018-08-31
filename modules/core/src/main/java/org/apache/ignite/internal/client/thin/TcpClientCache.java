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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

import static java.util.AbstractMap.SimpleEntry;
import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.CURRENT_VER;

/**
 * Implementation of {@link ClientCache} over TCP protocol.
 */
class TcpClientCache<K, V> implements ClientCache<K, V> {
    /** Cache id. */
    private final int cacheId;

    /** Channel. */
    private final ReliableChannel ch;

    /** Cache name. */
    private final String name;

    /** Marshaller. */
    private final ClientBinaryMarshaller marsh;

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    /** Indicates if cache works with Ignite Binary format. */
    private boolean keepBinary = false;

    /** Constructor. */
    TcpClientCache(String name, ReliableChannel ch, ClientBinaryMarshaller marsh) {
        this.name = name;
        this.cacheId = ClientUtils.cacheId(name);
        this.ch = ch;
        this.marsh = marsh;

        serDes = new ClientUtils(marsh);
    }

    /** {@inheritDoc} */
    @Override public V get(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return ch.service(
            ClientOperation.CACHE_GET,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
            },
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        ch.request(
            ClientOperation.CACHE_PUT,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
                serDes.writeObject(req, val);
            }
        );
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return ch.service(
            ClientOperation.CACHE_CONTAINS_KEY,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
            },
            BinaryInputStream::readBoolean
        );
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public ClientCacheConfiguration getConfiguration() throws ClientException {
        return ch.service(
            ClientOperation.CACHE_GET_CONFIGURATION,
            this::writeCacheInfo,
            res -> {
                try {
                    return serDes.cacheConfiguration(res, CURRENT_VER);
                }
                catch (IOException e) {
                    return null;
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws ClientException {
        return ch.service(
            ClientOperation.CACHE_GET_SIZE,
            req -> {
                writeCacheInfo(req);
                ClientUtils.collection(peekModes, req, (out, m) -> out.writeByte((byte)m.ordinal()));
            },
            res -> (int)res.readLong()
        );
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.size() == 0)
            return new HashMap<>();

        return ch.service(
            ClientOperation.CACHE_GET_ALL,
            req -> {
                writeCacheInfo(req);
                ClientUtils.collection(keys, req, serDes::writeObject);
            },
            res -> ClientUtils.collection(
                res,
                in -> new SimpleEntry<K, V>(readObject(in), readObject(in))
            )
        ).stream().collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) throws ClientException {
        if (map == null)
            throw new NullPointerException("map");

        if (map.size() == 0)
            return;

        ch.request(
            ClientOperation.CACHE_PUT_ALL,
            req -> {
                writeCacheInfo(req);
                ClientUtils.collection(
                    map.entrySet(),
                    req,
                    (out, e) -> {
                        serDes.writeObject(out, e.getKey());
                        serDes.writeObject(out, e.getValue());
                    });
            }
        );
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (oldVal == null)
            throw new NullPointerException("oldVal");

        if (newVal == null)
            throw new NullPointerException("newVal");

        return ch.service(
            ClientOperation.CACHE_REPLACE_IF_EQUALS,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
                serDes.writeObject(req, oldVal);
                serDes.writeObject(req, newVal);
            },
            BinaryInputStream::readBoolean
        );
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return ch.service(
            ClientOperation.CACHE_REPLACE,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
                serDes.writeObject(req, val);
            },
            BinaryInputStream::readBoolean
        );
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return ch.service(
            ClientOperation.CACHE_REMOVE_KEY,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
            },
            BinaryInputStream::readBoolean
        );
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (oldVal == null)
            throw new NullPointerException("oldVal");

        return ch.service(
            ClientOperation.CACHE_REMOVE_IF_EQUALS,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
                serDes.writeObject(req, oldVal);
            },
            BinaryInputStream::readBoolean
        );
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.size() == 0)
            return;

        ch.request(
            ClientOperation.CACHE_REMOVE_KEYS,
            req -> {
                writeCacheInfo(req);
                ClientUtils.collection(keys, req, serDes::writeObject);
            }
        );
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws ClientException {
        ch.request(ClientOperation.CACHE_REMOVE_ALL, this::writeCacheInfo);
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return ch.service(
            ClientOperation.CACHE_GET_AND_PUT,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
                serDes.writeObject(req, val);
            },
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return ch.service(
            ClientOperation.CACHE_GET_AND_REMOVE,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
            },
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return ch.service(
            ClientOperation.CACHE_GET_AND_REPLACE,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
                serDes.writeObject(req, val);
            },
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return ch.service(
            ClientOperation.CACHE_PUT_IF_ABSENT,
            req -> {
                writeCacheInfo(req);
                serDes.writeObject(req, key);
                serDes.writeObject(req, val);
            },
            BinaryInputStream::readBoolean
        );
    }

    /** {@inheritDoc} */
    @Override public void clear() throws ClientException {
        ch.request(ClientOperation.CACHE_CLEAR, this::writeCacheInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K1, V1> ClientCache<K1, V1> withKeepBinary() {
        TcpClientCache<K1, V1> binCache;

        if (keepBinary) {
            try {
                binCache = (TcpClientCache<K1, V1>)this;
            }
            catch (ClassCastException ex) {
                throw new IllegalStateException(
                    "Trying to enable binary mode on already binary cache with different key/value type arguments.",
                    ex
                );
            }
        }
        else {
            binCache = new TcpClientCache<>(name, ch, marsh);

            binCache.keepBinary = true;
        }

        return binCache;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        if (qry == null)
            throw new NullPointerException("qry");

        QueryCursor<R> res;

        if (qry instanceof ScanQuery)
            res = scanQuery((ScanQuery)qry);
        else if (qry instanceof SqlQuery)
            res = (QueryCursor<R>)sqlQuery((SqlQuery)qry);
        else if (qry instanceof SqlFieldsQuery)
            res = (QueryCursor<R>)query((SqlFieldsQuery)qry);
        else
            throw new IllegalArgumentException(
                String.format("Query of type [%s] is not supported", qry.getClass().getSimpleName())
            );

        return res;
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        if (qry == null)
            throw new NullPointerException("qry");

        Consumer<BinaryOutputStream> qryWriter = out -> {
            writeCacheInfo(out);
            serDes.write(qry, out);
        };

        return new ClientFieldsQueryCursor<>(new ClientFieldsQueryPager(
            ch,
            ClientOperation.QUERY_SQL_FIELDS,
            ClientOperation.QUERY_SQL_FIELDS_CURSOR_GET_PAGE,
            qryWriter,
            keepBinary,
            marsh
        ));
    }

    /** Handle scan query. */
    private QueryCursor<Cache.Entry<K, V>> scanQuery(ScanQuery<K, V> qry) {
        Consumer<BinaryOutputStream> qryWriter = out -> {
            writeCacheInfo(out);

            if (qry.getFilter() == null)
                out.writeByte(GridBinaryMarshaller.NULL);
            else {
                serDes.writeObject(out, qry.getFilter());
                out.writeByte((byte)1); // Java platform
            }

            out.writeInt(qry.getPageSize());
            out.writeInt(qry.getPartition() == null ? -1 : qry.getPartition());
            out.writeBoolean(qry.isLocal());
        };

        return new ClientQueryCursor<>(new ClientQueryPager<>(
            ch,
            ClientOperation.QUERY_SCAN,
            ClientOperation.QUERY_SCAN_CURSOR_GET_PAGE,
            qryWriter,
            keepBinary,
            marsh
        ));
    }

    /** Handle SQL query. */
    private QueryCursor<Cache.Entry<K, V>> sqlQuery(SqlQuery qry) {
        Consumer<BinaryOutputStream> qryWriter = out -> {
            writeCacheInfo(out);
            serDes.writeObject(out, qry.getType());
            serDes.writeObject(out, qry.getSql());
            ClientUtils.collection(qry.getArgs(), out, serDes::writeObject);
            out.writeBoolean(qry.isDistributedJoins());
            out.writeBoolean(qry.isLocal());
            out.writeBoolean(qry.isReplicatedOnly());
            out.writeInt(qry.getPageSize());
            out.writeLong(qry.getTimeout());
        };

        return new ClientQueryCursor<>(new ClientQueryPager<>(
            ch,
            ClientOperation.QUERY_SQL,
            ClientOperation.QUERY_SQL_CURSOR_GET_PAGE,
            qryWriter,
            keepBinary,
            marsh
        ));
    }

    /** Write cache ID and flags. */
    private void writeCacheInfo(BinaryOutputStream out) {
        out.writeInt(cacheId);
        out.writeByte((byte)(keepBinary ? 1 : 0));
    }

    /** */
    private <T> T readObject(BinaryInputStream in) {
        return serDes.readObject(in, keepBinary);
    }
}
