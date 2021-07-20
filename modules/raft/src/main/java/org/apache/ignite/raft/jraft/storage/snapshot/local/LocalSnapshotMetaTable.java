/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage.snapshot.local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import org.apache.ignite.raft.jraft.entity.LocalSnapshotPbMetaBuilder;
import org.apache.ignite.raft.jraft.entity.LocalStorageOutter.LocalSnapshotPbMeta;
import org.apache.ignite.raft.jraft.entity.LocalStorageOutter.LocalSnapshotPbMeta.File;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.io.MessageFile;
import org.apache.ignite.raft.jraft.util.Marshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table to keep local snapshot metadata infos.
 */
public class LocalSnapshotMetaTable {

    private static final Logger LOG = LoggerFactory.getLogger(LocalSnapshotMetaTable.class);

    private final Map<String, LocalFileMeta> fileMap;
    private final RaftOptions raftOptions;
    private SnapshotMeta meta;

    public LocalSnapshotMetaTable(RaftOptions raftOptions) {
        super();
        this.fileMap = new HashMap<>();
        this.raftOptions = raftOptions;
    }

    /**
     * Save metadata infos into byte buffer.
     */
    public ByteBuffer saveToByteBufferAsRemote() {
        final LocalSnapshotPbMetaBuilder pbMetaBuilder = raftOptions.getRaftMessagesFactory().localSnapshotPbMeta();
        if (hasMeta()) {
            pbMetaBuilder.meta(this.meta);
        }

        List<File> files = fileMap.entrySet().stream()
            .map(e -> raftOptions.getRaftMessagesFactory()
                .file()
                .name(e.getKey())
                .meta(e.getValue())
                .build()
            )
            .collect(Collectors.toList());

        pbMetaBuilder.filesList(files);

        return ByteBuffer.wrap(Marshaller.DEFAULT.marshall(pbMetaBuilder.build()));
    }

    /**
     * Load metadata infos from byte buffer.
     */
    public boolean loadFromIoBufferAsRemote(final ByteBuffer buf) {
        if (buf == null) {
            LOG.error("Null buf to load.");
            return false;
        }
        try {
            final LocalSnapshotPbMeta pbMeta = Marshaller.DEFAULT.unmarshall(buf.array());
            if (pbMeta == null) {
                LOG.error("Fail to load meta from buffer.");
                return false;
            }
            return loadFromPbMeta(pbMeta);
        }
        catch (final Exception e) {
            LOG.error("Fail to parse LocalSnapshotPbMeta from byte buffer", e);
            return false;
        }
    }

    /**
     * Adds a file metadata.
     */
    public boolean addFile(final String fileName, final LocalFileMeta meta) {
        return this.fileMap.putIfAbsent(fileName, meta) == null;
    }

    /**
     * Removes a file metadata.
     */
    public boolean removeFile(final String fileName) {
        return this.fileMap.remove(fileName) != null;
    }

    /**
     * Save metadata infos into file by path.
     */
    public boolean saveToFile(String path) throws IOException {
        LocalSnapshotPbMetaBuilder pbMeta = raftOptions.getRaftMessagesFactory().localSnapshotPbMeta();
        if (hasMeta()) {
            pbMeta.meta(this.meta);
        }

        List<File> files = fileMap.entrySet().stream()
            .map(e -> raftOptions.getRaftMessagesFactory()
                .file()
                .name(e.getKey())
                .meta(e.getValue())
                .build()
            )
            .collect(Collectors.toList());

        pbMeta.filesList(files);

        MessageFile pbFile = new MessageFile(path);
        return pbFile.save(pbMeta.build(), this.raftOptions.isSyncMeta());
    }

    /**
     * Returns true when has the snapshot metadata.
     */
    public boolean hasMeta() {
        return this.meta != null;
    }

    /**
     * Get the file metadata by fileName, returns null when not found.
     */
    public LocalFileMeta getFileMeta(String fileName) {
        return this.fileMap.get(fileName);
    }

    /**
     * Get all fileNames in this table.
     */
    public Set<String> listFiles() {
        return this.fileMap.keySet();
    }

    /**
     * Set the snapshot metadata.
     */
    public void setMeta(SnapshotMeta meta) {
        this.meta = meta;
    }

    /**
     * Returns the snapshot metadata.
     */
    public SnapshotMeta getMeta() {
        return this.meta;
    }

    /**
     * Load metadata infos from a file by path.
     */
    public boolean loadFromFile(String path) throws IOException {
        MessageFile pbFile = new MessageFile(path);
        LocalSnapshotPbMeta pbMeta = pbFile.load();
        if (pbMeta == null) {
            LOG.error("Fail to load meta from {}.", path);
            return false;
        }
        return loadFromPbMeta(pbMeta);
    }

    private boolean loadFromPbMeta(final LocalSnapshotPbMeta pbMeta) {
        if (pbMeta.meta() != null) {
            this.meta = pbMeta.meta();
        }
        else {
            this.meta = null;
        }
        this.fileMap.clear();
        for (final File f : pbMeta.filesList()) {
            this.fileMap.put(f.name(), f.meta());
        }
        return true;
    }
}
