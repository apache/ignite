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

package org.apache.ignite.internal.igfs.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.AFFINITY;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.OPEN_CREATE;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.OPEN_READ;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.READ_BLOCK;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.SET_TIMES;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.WRITE_BLOCK;

/**
 * Implementation of IGFS client message marshaller.
 */
public class IgfsMarshaller {
    /** Packet header size. */
    public static final int HEADER_SIZE = 24;

    /**
     * Creates new header with given request ID and command.
     *
     * @param reqId Request ID.
     * @param cmd Command.
     * @return Created header.
     */
    public static byte[] createHeader(long reqId, IgfsIpcCommand cmd) {
        assert cmd != null;

        byte[] hdr = new byte[HEADER_SIZE];

        U.longToBytes(reqId, hdr, 0);

        U.intToBytes(cmd.ordinal(), hdr, 8);

        return hdr;
    }

    /**
     * Creates new header with given request ID and command.
     *
     * @param reqId Request ID.
     * @param cmd Command.
     * @return Created header.
     */
    public static byte[] fillHeader(byte[] hdr, long reqId, IgfsIpcCommand cmd) {
        assert cmd != null;

        Arrays.fill(hdr, (byte)0);

        U.longToBytes(reqId, hdr, 0);

        U.intToBytes(cmd.ordinal(), hdr, 8);

        return hdr;
    }

    /**
     * Serializes the message and sends it into the given output stream.
     * @param msg Message.
     * @param hdr Message header.
     * @param out Output.
     * @throws IgniteCheckedException If failed.
     */
    public void marshall(IgfsMessage msg, byte[] hdr, ObjectOutput out) throws IgniteCheckedException {
        assert hdr != null;
        assert hdr.length == HEADER_SIZE;

        try {
            switch (msg.command()) {
                case HANDSHAKE: {
                    out.write(hdr);

                    IgfsHandshakeRequest req = (IgfsHandshakeRequest)msg;

                    U.writeString(out, req.gridName());
                    U.writeString(out, req.igfsName());
                    U.writeString(out, req.logDirectory());

                    break;
                }
                case STATUS: {
                    out.write(hdr);

                    break;
                }

                case EXISTS:
                case INFO:
                case PATH_SUMMARY:
                case UPDATE:
                case RENAME:
                case DELETE:
                case MAKE_DIRECTORIES:
                case LIST_PATHS:
                case LIST_FILES:
                case AFFINITY:
                case SET_TIMES:
                case OPEN_READ:
                case OPEN_APPEND:
                case OPEN_CREATE: {
                    out.write(hdr);

                    IgfsPathControlRequest req = (IgfsPathControlRequest)msg;

                    U.writeString(out, req.userName());
                    writePath(out, req.path());
                    writePath(out, req.destinationPath());
                    out.writeBoolean(req.flag());
                    out.writeBoolean(req.colocate());
                    U.writeStringMap(out, req.properties());

                    // Minor optimization.
                    if (msg.command() == AFFINITY) {
                        out.writeLong(req.start());
                        out.writeLong(req.length());
                    }
                    else if (msg.command() == OPEN_CREATE) {
                        out.writeInt(req.replication());
                        out.writeLong(req.blockSize());
                    }
                    else if (msg.command() == SET_TIMES) {
                        out.writeLong(req.accessTime());
                        out.writeLong(req.modificationTime());
                    }
                    else if (msg.command() == OPEN_READ && req.flag())
                        out.writeInt(req.sequentialReadsBeforePrefetch());

                    break;
                }

                case CLOSE:
                case READ_BLOCK:
                case WRITE_BLOCK: {
                    assert msg.command() != WRITE_BLOCK : "WRITE_BLOCK should be marshalled manually.";

                    IgfsStreamControlRequest req = (IgfsStreamControlRequest)msg;

                    U.longToBytes(req.streamId(), hdr, 12);

                    if (msg.command() == READ_BLOCK)
                        U.intToBytes(req.length(), hdr, 20);

                    out.write(hdr);

                    if (msg.command() == READ_BLOCK)
                        out.writeLong(req.position());

                    break;
                }

                case CONTROL_RESPONSE: {
                    out.write(hdr);

                    IgfsControlResponse res = (IgfsControlResponse)msg;

                    res.writeExternal(out);

                    break;
                }

                default: {
                    assert false : "Invalid command: " + msg.command();

                    throw new IllegalArgumentException("Failed to marshal message (invalid command): " +
                        msg.command());
                }
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to send message to IGFS data node (is data node up and running?)", e);
        }
    }

    /**
     * @param cmd Command.
     * @param hdr Header.
     * @param in Input.
     * @return Message.
     * @throws IgniteCheckedException If failed.
     */
    public IgfsMessage unmarshall(IgfsIpcCommand cmd, byte[] hdr, ObjectInput in) throws IgniteCheckedException {
        assert hdr != null;
        assert hdr.length == HEADER_SIZE;

        try {
            IgfsMessage msg;

            switch (cmd) {
                case HANDSHAKE: {
                    IgfsHandshakeRequest req = new IgfsHandshakeRequest();

                    req.gridName(U.readString(in));
                    req.igfsName(U.readString(in));
                    req.logDirectory(U.readString(in));

                    msg = req;

                    break;
                }

                case STATUS: {
                    msg = new IgfsStatusRequest();

                    break;
                }

                case EXISTS:
                case INFO:
                case PATH_SUMMARY:
                case UPDATE:
                case RENAME:
                case DELETE:
                case MAKE_DIRECTORIES:
                case LIST_PATHS:
                case LIST_FILES:
                case SET_TIMES:
                case AFFINITY:
                case OPEN_READ:
                case OPEN_APPEND:
                case OPEN_CREATE: {
                    IgfsPathControlRequest req = new IgfsPathControlRequest();

                    req.userName(U.readString(in));
                    req.path(readPath(in));
                    req.destinationPath(readPath(in));
                    req.flag(in.readBoolean());
                    req.colocate(in.readBoolean());
                    req.properties(U.readStringMap(in));

                    // Minor optimization.
                    if (cmd == AFFINITY) {
                        req.start(in.readLong());
                        req.length(in.readLong());
                    }
                    else if (cmd == OPEN_CREATE) {
                        req.replication(in.readInt());
                        req.blockSize(in.readLong());
                    }
                    else if (cmd == SET_TIMES) {
                        req.accessTime(in.readLong());
                        req.modificationTime(in.readLong());
                    }
                    else if (cmd == OPEN_READ && req.flag())
                        req.sequentialReadsBeforePrefetch(in.readInt());

                    msg = req;

                    break;
                }

                case CLOSE:
                case READ_BLOCK:
                case WRITE_BLOCK: {
                    IgfsStreamControlRequest req = new IgfsStreamControlRequest();

                    long streamId = U.bytesToLong(hdr, 12);

                    req.streamId(streamId);
                    req.length(U.bytesToInt(hdr, 20));

                    if (cmd == READ_BLOCK)
                        req.position(in.readLong());

                    msg = req;

                    break;
                }

                case CONTROL_RESPONSE: {
                    IgfsControlResponse res = new IgfsControlResponse();

                    res.readExternal(in);

                    msg = res;

                    break;
                }

                default: {
                    assert false : "Invalid command: " + cmd;

                    throw new IllegalArgumentException("Failed to unmarshal message (invalid command): " + cmd);
                }
            }

            msg.command(cmd);

            return msg;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IgniteCheckedException("Failed to unmarshal client message: " + cmd, e);
        }
    }

    /**
     * Writes IGFS path to given data output. Can write {@code null} values.
     *
     * @param out Data output.
     * @param path Path to write.
     * @throws IOException If write failed.
     */
    private void writePath(ObjectOutput out, @Nullable IgfsPath path) throws IOException {
        out.writeBoolean(path != null);

        if (path != null)
            path.writeExternal(out);
    }

    /**
     * Reads IGFS path from data input that was written by {@link #writePath(ObjectOutput, org.apache.ignite.igfs.IgfsPath)}
     * method.
     *
     * @param in Data input.
     * @return Written path or {@code null}.
     */
    @Nullable private IgfsPath readPath(ObjectInput in) throws IOException {
        if(in.readBoolean()) {
            IgfsPath path = new IgfsPath();

            path.readExternal(in);

            return path;
        }

        return null;
    }
}