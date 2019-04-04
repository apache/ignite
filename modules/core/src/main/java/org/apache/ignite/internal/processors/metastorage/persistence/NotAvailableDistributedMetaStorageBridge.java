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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;
import java.util.function.BiConsumer;

/** */
class NotAvailableDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
    /** {@inheritDoc} */
    @Override public Serializable read(String globalKey, boolean unmarshal) {
        throw new UnsupportedOperationException("read");
    }

    /** {@inheritDoc} */
    @Override public void iterate(
        String globalKeyPre,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) {
        throw new UnsupportedOperationException("iterate");
    }

    /** {@inheritDoc} */
    @Override public void write(String globalKey, byte[] valBytes) {
        throw new UnsupportedOperationException("write");
    }

    /** {@inheritDoc} */
    @Override public void onUpdateMessage(DistributedMetaStorageHistoryItem histItem) {
        throw new UnsupportedOperationException("onUpdateMessage");
    }

    /** {@inheritDoc} */
    @Override public void removeHistoryItem(long ver) {
        throw new UnsupportedOperationException("removeHistoryItem");
    }

    /** {@inheritDoc} */
    @Override public DistributedMetaStorageKeyValuePair[] localFullData() {
        throw new UnsupportedOperationException("localFullData");
    }
}
