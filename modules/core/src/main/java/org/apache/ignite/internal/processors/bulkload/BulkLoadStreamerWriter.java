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

package org.apache.ignite.internal.processors.bulkload;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * A bulk load cache writer object that adds entries using {@link IgniteDataStreamer}.
 */
public class BulkLoadStreamerWriter extends BulkLoadCacheWriter {
    /** Serialization version UID. */
    private static final long serialVersionUID = 0L;

    /** The streamer. */
    private final IgniteDataStreamer<Object, Object> streamer;

    /**
     * A number of {@link IgniteDataStreamer#addData(Object, Object)} calls made,
     * since we don't have any kind of result data back from the streamer.
     */
    private long updateCnt;

    /**
     * Creates a cache writer.
     *
     * @param streamer The streamer to use.
     */
    public BulkLoadStreamerWriter(IgniteDataStreamer<Object, Object> streamer) {
        this.streamer = streamer;
        updateCnt = 0;
    }

    /** {@inheritDoc} */
    @Override public void apply(IgniteBiTuple<?, ?> entry) {
        streamer.addData(entry.getKey(), entry.getValue());

        updateCnt++;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        streamer.close();
    }

    /** {@inheritDoc} */
    @Override public long updateCnt() {
        return updateCnt;
    }
}
