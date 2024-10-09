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

import java.util.Collection;
import java.util.function.Consumer;
import javax.cache.Cache;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.client.thin.TcpClientTransactions.TcpClientTransaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client query pager.
 */
class ClientQueryPager<K, V> extends GenericQueryPager<Cache.Entry<K, V>> {
    /** Keep binary. */
    private final boolean keepBinary;

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    /** Constructor. */
    ClientQueryPager(
        ReliableChannel ch,
        @Nullable TcpClientTransaction tx,
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<PayloadOutputChannel> qryWriter,
        boolean keepBinary,
        ClientBinaryMarshaller marsh
    ) {
        super(ch, tx, qryOp, pageQryOp, qryWriter);

        this.keepBinary = keepBinary;

        serDes = new ClientUtils(marsh);
    }

    /** Constructor. */
    ClientQueryPager(
        ReliableChannel ch,
        @Nullable TcpClientTransaction tx,
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<PayloadOutputChannel> qryWriter,
        boolean keepBinary,
        ClientBinaryMarshaller marsh,
        int cacheId,
        int part
    ) {
        super(ch, tx, qryOp, pageQryOp, qryWriter, cacheId, part);

        this.keepBinary = keepBinary;

        serDes = new ClientUtils(marsh);
    }

    /** {@inheritDoc} */
    @Override Collection<Cache.Entry<K, V>> readEntries(PayloadInputChannel paloadCh) {
        BinaryInputStream in = paloadCh.in();

        return ClientUtils.collection(
            in,
            ignored -> new ClientCacheEntry<>(serDes.readObject(in, keepBinary), serDes.readObject(in, keepBinary))
        );
    }
}

