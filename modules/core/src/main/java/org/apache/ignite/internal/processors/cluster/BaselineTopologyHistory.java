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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;

/**
 *
 */
public class BaselineTopologyHistory {
    /** */
    private static final String METASTORE_BLT_HIST_PREFIX = "bltHist-";

    /** */
    private final List<BaselineTopologyHistoryItem> hist = new ArrayList<>();

    /** */
    public void restoreHistory(ReadOnlyMetastorage metastorage, int lastId) throws IgniteCheckedException {
        for (int i = 0; i < lastId; i++) {
            BaselineTopologyHistoryItem histItem = (BaselineTopologyHistoryItem) metastorage.read(METASTORE_BLT_HIST_PREFIX + i);

            if (histItem != null)
                hist.add(histItem);
            else {
                //TODO figure out how to recover from this
            }
        }
    }

    /** */
    void addHistoryItem(ReadWriteMetastorage metastorage, BaselineTopologyHistoryItem histItem)
        throws IgniteCheckedException
    {
        if (histItem == null)
            return;

        hist.add(histItem);

        metastorage.write(METASTORE_BLT_HIST_PREFIX + histItem.id(), histItem);
    }

    /** */
    boolean isCompatibleWith(BaselineTopology blt) {
        BaselineTopologyHistoryItem histBlt = hist.get(blt.id());

        return histBlt.activationHistory().contains(blt.activationHash());
    }
}
