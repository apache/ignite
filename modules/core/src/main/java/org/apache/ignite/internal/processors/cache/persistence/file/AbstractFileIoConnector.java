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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public abstract class AbstractFileIoConnector implements AutoCloseable {
    /** */
    protected final SocketChannel channel;

    /** */
    protected final IgniteLogger log;

    /** */
    protected final FileIOFactory factory;

    /** */
    private final DataInputStream dis;

    /** */
    private final DataOutputStream dos;

    /** */
    protected AbstractFileIoConnector(
        SocketChannel channel,
        FileIOFactory factory,
        IgniteLogger log
    ) throws IOException {
        this.channel = channel;
        this.dis = new DataInputStream(new BufferedInputStream(channel.socket().getInputStream()));
        this.dos = new DataOutputStream(new BufferedOutputStream(channel.socket().getOutputStream()));
        this.factory = factory;
        this.log = log;
    }

    /**
     * @return Recevied metadata info.
     * @throws IOException If fails.
     */
    public T2<String, Long> readFileMeta() throws IOException {
        U.log(log, "Reading file metadata from remote: " + channel.getRemoteAddress());

        String name = dis.readUTF();
        Long size = dis.readLong();

        // CRC32 ?

        if (name == null || size == null)
            throw new IOException("Unable to recieve file metadata from the remote node.");

        return new T2<>(name, size);

    }

    /**
     * @param name File name to write it back.
     * @param size File size to write it back.
     * @throws IOException If fails.
     */
    public void writeFileMeta(String name, Long size) throws IOException {
        U.log(log, "Writing file metadata to remote [name=" + name + ", size=" + size +
            ", remote=" + channel.getRemoteAddress() + ']');

        dos.writeUTF(name);
        dos.writeLong(size);

        dos.flush();
    }

    /** */
    public void sendFileIoAck() {

    }

    /** */
    public void receiveFileIoAck() {

    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        channel.close();
        dos.close();
        dis.close();
    }
}
