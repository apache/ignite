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

package org.gridgain.loadtests.streamer;

import org.apache.ignite.streamer.index.*;
import org.jetbrains.annotations.*;

/**
 * Streamer benchmark window index updater.
 */
class IndexUpdater implements StreamerIndexUpdater<Integer, Integer, Long> {
    /** {@inheritDoc} */
    @Override public Integer indexKey(Integer evt) {
        return evt;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Long onAdded(StreamerIndexEntry<Integer, Integer, Long> entry, Integer evt) {
        return entry.value() + 1;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Long onRemoved(StreamerIndexEntry<Integer, Integer, Long> entry, Integer evt) {
        return entry.value() - 1 == 0 ? null : entry.value() - 1;
    }

    /** {@inheritDoc} */
    @Override public Long initialValue(Integer evt, Integer key) {
        return 1L;
    }
}
