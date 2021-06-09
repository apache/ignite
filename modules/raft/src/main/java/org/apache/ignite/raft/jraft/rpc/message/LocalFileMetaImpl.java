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

import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.rpc.Message;

public class LocalFileMetaImpl implements LocalFileMetaOutter.LocalFileMeta, LocalFileMetaOutter.LocalFileMeta.Builder {
    private LocalFileMetaOutter.FileSource fileSource;
    private String checksum;

    @Override public LocalFileMetaOutter.FileSource getSource() {
        return fileSource;
    }

    @Override public String getChecksum() {
        return checksum;
    }

    @Override public boolean hasChecksum() {
        return checksum != null;
    }

    @Override public boolean hasUserMeta() {
        return false;
    }

    @Override public LocalFileMetaOutter.LocalFileMeta build() {
        return this;
    }

    @Override public void mergeFrom(Message fileMeta) {
        LocalFileMetaOutter.LocalFileMeta tmp = (LocalFileMetaOutter.LocalFileMeta) fileMeta;

        this.fileSource = tmp.getSource();
        this.checksum = tmp.getChecksum();
    }

    @Override public Builder setChecksum(String checksum) {
        this.checksum = checksum;

        return this;
    }

    @Override public Builder setSource(LocalFileMetaOutter.FileSource source) {
        this.fileSource = source;

        return this;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LocalFileMetaImpl that = (LocalFileMetaImpl) o;

        if (fileSource != that.fileSource)
            return false;
        return checksum.equals(that.checksum);
    }

    @Override public int hashCode() {
        int result = fileSource.hashCode();
        result = 31 * result + checksum.hashCode();
        return result;
    }
}
