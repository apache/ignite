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
package org.apache.ignite.raft.jraft.rpc.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.jraft.entity.LocalStorageOutter;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.util.Marshaller;

class LocalSnapshotMetaImpl implements LocalStorageOutter.LocalSnapshotPbMeta, LocalStorageOutter.LocalSnapshotPbMeta.Builder {
    private RaftOutter.SnapshotMeta meta;
    private List<File> files = new ArrayList<>();

    @Override public RaftOutter.SnapshotMeta getMeta() {
        return meta;
    }

    @Override public List<File> getFilesList() {
        return files;
    }

    @Override public int getFilesCount() {
        return files.size();
    }

    @Override public File getFiles(int index) {
        return files.get(index);
    }

    @Override public byte[] toByteArray() {
        return Marshaller.DEFAULT.marshall(this);
    }

    @Override public boolean hasMeta() {
        return meta != null;
    }

    @Override public Builder setMeta(RaftOutter.SnapshotMeta meta) {
        this.meta = meta;

        return this;
    }

    @Override public Builder addFiles(File file) {
        files.add(file);

        return this;
    }

    @Override public LocalStorageOutter.LocalSnapshotPbMeta build() {
        return this;
    }
}
