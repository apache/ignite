/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<PayloadOutputChannel> qryWriter,
        boolean keepBinary,
        ClientBinaryMarshaller marsh
    ) {
        super(ch, qryOp, pageQryOp, qryWriter);

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

