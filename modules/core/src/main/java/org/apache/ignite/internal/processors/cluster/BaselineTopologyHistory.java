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
package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;

/**
 *
 */
public class BaselineTopologyHistory implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final String METASTORE_BLT_HIST_PREFIX = "bltHist-";

    /** */
    private final Queue<BaselineTopologyHistoryItem> bufferedForStore =
        new ConcurrentLinkedQueue<>();

    /** */
    private final List<BaselineTopologyHistoryItem> hist = new ArrayList<>();

    /** */
    void restoreHistory(ReadOnlyMetastorage metastorage, int lastId) throws IgniteCheckedException {
        for (int i = 0; i < lastId; i++) {
            BaselineTopologyHistoryItem histItem = (BaselineTopologyHistoryItem) metastorage.read(METASTORE_BLT_HIST_PREFIX + i);

            if (histItem != null)
                hist.add(histItem);
            else
                throw new IgniteCheckedException("Restoring of BaselineTopology history has failed, " +
                    "expected history item not found for id=" + i
                );
        }
    }

    /** */
    BaselineTopologyHistory tailFrom(int id) {
        BaselineTopologyHistory tail = new BaselineTopologyHistory();

        for (BaselineTopologyHistoryItem item : hist) {
            if (item.id() >= id)
                tail.hist.add(item);
        }

        return tail;
    }

    /** */
    void writeHistoryItem(ReadWriteMetastorage metastorage, BaselineTopologyHistoryItem histItem)
        throws IgniteCheckedException
    {
        if (histItem == null)
            return;

        hist.add(histItem);

        metastorage.write(METASTORE_BLT_HIST_PREFIX + histItem.id(), histItem);
    }

    /** */
    void removeHistory(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        if (hist.isEmpty())
            return;

        for (BaselineTopologyHistoryItem histItem : hist)
            metastorage.remove(METASTORE_BLT_HIST_PREFIX + histItem.id());

        hist.clear();
    }

    /** */
    boolean isCompatibleWith(BaselineTopology blt) {
        BaselineTopologyHistoryItem histBlt = hist.get(blt.id());

        return histBlt.branchingHistory().contains(blt.branchingPointHash());
    }

    /** */
    boolean isEmpty() {
        return hist.isEmpty();
    }

    /** */
    void bufferHistoryItemForStore(BaselineTopologyHistoryItem histItem) {
        hist.add(histItem);

        bufferedForStore.add(histItem);
    }

    /** */
    public List<BaselineTopologyHistoryItem> history() {
        return Collections.unmodifiableList(hist);
    }

    /** */
    void flushHistoryItems(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        while (!bufferedForStore.isEmpty()) {
            BaselineTopologyHistoryItem item = bufferedForStore.remove();

            metastorage.write(METASTORE_BLT_HIST_PREFIX + item.id(), item);
        }
    }
}
