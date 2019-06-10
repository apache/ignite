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

package org.apache.ignite.ml.inference.storage.model.thinclient;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.ml.inference.storage.model.FileStat;

/**
 * File statistics repsponse.
 */
public class FileStatResponse extends ClientResponse {
    /** File statistics. */
    private final FileStat stat;

    /**
     * Create an instance of file statistics.
     *
     * @param reqId Request id.
     * @param stat Statistics.
     */
    public FileStatResponse(long reqId, FileStat stat) {
        super(reqId);

        this.stat = stat;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeBoolean(stat.isDirectory());
        writer.writeInt(stat.getSize());
        writer.writeLong(stat.getModificationTime());
    }

    /**
     * @return Returns file's stat.
     */
    FileStat getStat() {
        return stat;
    }
}
