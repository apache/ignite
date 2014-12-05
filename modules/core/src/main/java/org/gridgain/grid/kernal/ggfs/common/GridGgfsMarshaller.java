/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.common;

import org.apache.ignite.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.ggfs.common.GridGgfsIpcCommand.*;

/**
 * Implementation of GGFS client message marshaller.
 */
public class GridGgfsMarshaller {
    /** Packet header size. */
    public static final int HEADER_SIZE = 24;

    /**
     * Creates new header with given request ID and command.
     *
     * @param reqId Request ID.
     * @param cmd Command.
     * @return Created header.
     */
    public static byte[] createHeader(long reqId, GridGgfsIpcCommand cmd) {
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
    public static byte[] fillHeader(byte[] hdr, long reqId, GridGgfsIpcCommand cmd) {
        assert cmd != null;

        Arrays.fill(hdr, (byte)0);

        U.longToBytes(reqId, hdr, 0);

        U.intToBytes(cmd.ordinal(), hdr, 8);

        return hdr;
    }

    /**
     * @param msg Message.
     * @param hdr Message header.
     * @param out Output.
     * @throws GridException If failed.
     */
    public void marshall(GridGgfsMessage msg, byte[] hdr, ObjectOutput out) throws GridException {
        assert hdr != null;
        assert hdr.length == HEADER_SIZE;

        try {
            switch (msg.command()) {
                case HANDSHAKE: {
                    out.write(hdr);

                    GridGgfsHandshakeRequest req = (GridGgfsHandshakeRequest)msg;

                    U.writeString(out, req.gridName());
                    U.writeString(out, req.ggfsName());
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

                    GridGgfsPathControlRequest req = (GridGgfsPathControlRequest)msg;

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

                    GridGgfsStreamControlRequest req = (GridGgfsStreamControlRequest)msg;

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

                    GridGgfsControlResponse res = (GridGgfsControlResponse)msg;

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
            throw new GridException("Failed to send message to GGFS data node (is data node up and running?)", e);
        }
    }

    /**
     * @param cmd Command.
     * @param hdr Header.
     * @param in Input.
     * @return Message.
     * @throws GridException If failed.
     */
    public GridGgfsMessage unmarshall(GridGgfsIpcCommand cmd, byte[] hdr, ObjectInput in) throws GridException {
        assert hdr != null;
        assert hdr.length == HEADER_SIZE;

        try {
            GridGgfsMessage msg;

            switch (cmd) {
                case HANDSHAKE: {
                    GridGgfsHandshakeRequest req = new GridGgfsHandshakeRequest();

                    req.gridName(U.readString(in));
                    req.ggfsName(U.readString(in));
                    req.logDirectory(U.readString(in));

                    msg = req;

                    break;
                }

                case STATUS: {
                    msg = new GridGgfsStatusRequest();

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
                    GridGgfsPathControlRequest req = new GridGgfsPathControlRequest();

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
                    GridGgfsStreamControlRequest req = new GridGgfsStreamControlRequest();

                    long streamId = U.bytesToLong(hdr, 12);

                    req.streamId(streamId);
                    req.length(U.bytesToInt(hdr, 20));

                    if (cmd == READ_BLOCK)
                        req.position(in.readLong());

                    msg = req;

                    break;
                }

                case CONTROL_RESPONSE: {
                    GridGgfsControlResponse res = new GridGgfsControlResponse();

                    res.readExternal(in);

                    msg = res;

                    break;
                }

                default: {
                    assert false : "Invalid command: " + cmd;

                    throw new IllegalArgumentException("Failed to unmarshal message (invalid command): " + cmd);
                }
            }

            assert msg != null;

            msg.command(cmd);

            return msg;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new GridException("Failed to unmarshal client message: " + cmd, e);
        }
    }

    /**
     * Writes GGFS path to given data output. Can write {@code null} values.
     *
     * @param out Data output.
     * @param path Path to write.
     * @throws IOException If write failed.
     */
    private void writePath(ObjectOutput out, @Nullable IgniteFsPath path) throws IOException {
        out.writeBoolean(path != null);

        if (path != null)
            path.writeExternal(out);
    }

    /**
     * Reads GGFS path from data input that was written by {@link #writePath(ObjectOutput, org.apache.ignite.fs.IgniteFsPath)}
     * method.
     *
     * @param in Data input.
     * @return Written path or {@code null}.
     */
    @Nullable private IgniteFsPath readPath(ObjectInput in) throws IOException {
        if(in.readBoolean()) {
            IgniteFsPath path = new IgniteFsPath();

            path.readExternal(in);

            return path;
        }

        return null;
    }

    /**
     * Writes string to output.
     *
     * @param out Data output.
     * @param str String.
     * @throws IOException If write failed.
     */
    private void writeString(DataOutput out, @Nullable String str) throws IOException {
        out.writeBoolean(str != null);

        if (str != null)
            out.writeUTF(str);
    }

    /**
     * Reads string from input.
     *
     * @param in Data input.
     * @return Read string.
     * @throws IOException If read failed.
     */
    @Nullable private String readString(DataInput in) throws IOException {
        boolean hasStr = in.readBoolean();

        if (hasStr)
            return in.readUTF();

        return null;
    }
}
