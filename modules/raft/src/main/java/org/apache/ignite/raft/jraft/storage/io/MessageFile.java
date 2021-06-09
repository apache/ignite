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
package org.apache.ignite.raft.jraft.storage.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.util.Bits;
import org.apache.ignite.raft.jraft.util.Marshaller;
import org.apache.ignite.raft.jraft.util.Utils;

/**
 * TODO A file to store a message.
 */
public class MessageFile {
    /** file path */
    private final String path;

    public MessageFile(final String path) {
        this.path = path;
    }

    /**
     * Load a protobuf message from file.
     */
    public <T extends Message> T load() throws IOException {
        File file = new File(this.path);

        if (!file.exists()) {
            return null;
        }

        final byte[] lenBytes = new byte[4];
        try (final FileInputStream fin = new FileInputStream(file);
             final BufferedInputStream input = new BufferedInputStream(fin)) {
            readBytes(lenBytes, input);
            final int len = Bits.getInt(lenBytes, 0); // TODO asch endianess ?
            if (len <= 0) {
                throw new IOException("Invalid message fullName.");
            }
            final byte[] nameBytes = new byte[len];
            readBytes(nameBytes, input);
            return Marshaller.DEFAULT.unmarshall(nameBytes);
        }
    }

    private void readBytes(final byte[] bs, final InputStream input) throws IOException {
        int read;
        if ((read = input.read(bs)) != bs.length) {
            throw new IOException("Read error, expects " + bs.length + " bytes, but read " + read);
        }
    }

    /**
     * Save a message to file.
     *
     * @param msg protobuf message
     * @param sync if sync flush data to disk
     * @return true if save success
     */
    public boolean save(final Message msg, final boolean sync) throws IOException {
        // Write message into temp file
        final File file = new File(this.path + ".tmp");
        try (final FileOutputStream fOut = new FileOutputStream(file);
             final BufferedOutputStream output = new BufferedOutputStream(fOut)) {
            byte[] bytes = Marshaller.DEFAULT.marshall(msg);

            final byte[] lenBytes = new byte[4];
            Bits.putInt(lenBytes, 0, bytes.length);
            output.write(lenBytes);
            output.write(bytes);
            output.flush();
        }
        if (sync) {
            Utils.fsync(file);
        }

        return Utils.atomicMoveFile(file, new File(this.path), sync);
    }
}
