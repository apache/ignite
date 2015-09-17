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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsCorruptedFileException;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsInvalidHdfsVersionException;
import org.apache.ignite.igfs.IgfsParentNotDirectoryException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathAlreadyExistsException;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.internal.processors.igfs.IgfsBlockLocationImpl;
import org.apache.ignite.internal.processors.igfs.IgfsFileImpl;
import org.apache.ignite.internal.processors.igfs.IgfsHandshakeResponse;
import org.apache.ignite.internal.processors.igfs.IgfsInputStreamDescriptor;
import org.apache.ignite.internal.processors.igfs.IgfsStatus;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.CONTROL_RESPONSE;

/**
 * IGFS path command response.
 */
public class IgfsControlResponse extends IgfsMessage {
    /** Generic error (not IGFS) while performing operations. */
    private static final int ERR_GENERIC = 0;

    /** Generic IGFS error while performing operations. */
    private static final int ERR_IGFS_GENERIC = 1;

    /** Target file not found. */
    private static final int ERR_FILE_NOT_FOUND = 2;

    /** Target path already exists. */
    private static final int ERR_PATH_ALREADY_EXISTS = 3;

    /** Directory is not empty with */
    private static final int ERR_DIRECTORY_NOT_EMPTY = 4;

    /** Target parent is not a directory. */
    private static final int ERR_PARENT_NOT_DIRECTORY = 5;

    /** Secondary HDFS version differs from classpath version. */
    private static final int ERR_INVALID_HDFS_VERSION = 6;

    /** Failed to retrieve file's data block. */
    private static final int ERR_CORRUPTED_FILE = 7;

    /** Response is boolean. */
    public static final int RES_TYPE_BOOLEAN = 0;

    /** Response is Long. */
    public static final int RES_TYPE_LONG = 1;

    /** Response is IgfsFile. */
    public static final int RES_TYPE_IGFS_FILE = 2;

    /** Response is IgfsFileInfo. */
    public static final int RES_TYPE_IGFS_STREAM_DESCRIPTOR = 3;

    /** Response is IgfsPath. */
    public static final int RES_TYPE_IGFS_PATH = 4;

    /** Response is collection of IgfsFile. */
    public static final int RES_TYPE_COL_IGFS_FILE = 5;

    /** Response is collection of IgfsPath. */
    public static final int RES_TYPE_COL_IGFS_PATH = 6;

    /** Response is collection of IgfsBlockLocation. */
    public static final int RES_TYPE_COL_IGFS_BLOCK_LOCATION = 7;

    /** Response is collection of IgfsBlockLocation. */
    public static final int RES_TYPE_BYTE_ARRAY = 8;

    /** Response is an error containing stream ID and error message. */
    public static final int RES_TYPE_ERR_STREAM_ID = 9;

    /** Response is a handshake  */
    public static final int RES_TYPE_HANDSHAKE = 10;

    /** Response is a handshake  */
    public static final int RES_TYPE_STATUS = 11;

    /** Response is a path summary. */
    public static final int RES_TYPE_IGFS_PATH_SUMMARY = 12;

    /** Message header size. */
    public static final int RES_HEADER_SIZE = 9;

    /** We have limited number of object response types. */
    private int resType = -1;

    /** Response. */
    @GridToStringInclude
    private Object res;

    /** Bytes length to avoid iteration and summing. */
    private int len;

    /** Error (if any). */
    private String err;

    /** Error code. */
    private int errCode = -1;

    /**
     *
     */
    public IgfsControlResponse() {
        command(CONTROL_RESPONSE);
    }

    /**
     * @return Response.
     */
    public Object response() {
        return res;
    }

    /**
     * @param res Response.
     */
    public void response(boolean res) {
        resType = RES_TYPE_BOOLEAN;

        this.res = res;
    }

    /**
     * @param res Response.
     */
    public void response(long res) {
        resType = RES_TYPE_LONG;

        this.res = res;
    }

    /**
     * @param res Response.
     */
    public void response(byte[][] res) {
        resType = RES_TYPE_BYTE_ARRAY;

        this.res = res;
    }

    /**
     * @param res Response.
     */
    public void response(IgfsInputStreamDescriptor res) {
        resType = RES_TYPE_IGFS_STREAM_DESCRIPTOR;

        this.res = res;
    }

    /**
     * @param res Response.
     */
    public void response(IgfsFile res) {
        resType = RES_TYPE_IGFS_FILE;

        this.res = res;
    }

    /**
     * @param res Response.
     */
    public void response(IgfsPath res) {
        resType = RES_TYPE_IGFS_PATH;

        this.res = res;
    }

    /**
     * @param res Path summary response.
     */
    public void response(IgfsPathSummary res) {
        resType = RES_TYPE_IGFS_PATH_SUMMARY;

        this.res = res;
    }

    /**
     * @param res Response.
     */
    public void files(Collection<IgfsFile> res) {
        resType = RES_TYPE_COL_IGFS_FILE;

        this.res = res;
    }

    /**
     * @param res Response.
     */
    public void paths(Collection<IgfsPath> res) {
        resType = RES_TYPE_COL_IGFS_PATH;

        this.res = res;
    }

    /**
     * @param res Response.
     */
    public void locations(Collection<IgfsBlockLocation> res) {
        resType = RES_TYPE_COL_IGFS_BLOCK_LOCATION;

        this.res = res;
    }

    /**
     * @param res Handshake message.
     */
    public void handshake(IgfsHandshakeResponse res) {
        resType = RES_TYPE_HANDSHAKE;

        this.res = res;
    }

    /**
     * @param res Status response.
     */
    public void status(IgfsStatus res) {
        resType = RES_TYPE_STATUS;

        this.res = res;
    }

    /**
     * @param len Response length.
     */
    public void length(int len) {
        this.len = len;
    }

    /**
     * @return Error message if occurred.
     */
    public boolean hasError() {
        return errCode != -1;
    }

    /**
     * @param errCode Error code.
     * @param err Error.
     * @throws IgniteCheckedException Based on error code.
     */
    public static void throwError(Integer errCode, String err) throws IgniteCheckedException {
        assert err != null;
        assert errCode != -1;

        if (errCode == ERR_FILE_NOT_FOUND)
            throw new IgfsPathNotFoundException(err);
        else if (errCode == ERR_PATH_ALREADY_EXISTS)
            throw new IgfsPathAlreadyExistsException(err);
        else if (errCode == ERR_DIRECTORY_NOT_EMPTY)
            throw new IgfsDirectoryNotEmptyException(err);
        else if (errCode == ERR_PARENT_NOT_DIRECTORY)
            throw new IgfsParentNotDirectoryException(err);
        else if (errCode == ERR_INVALID_HDFS_VERSION)
            throw new IgfsInvalidHdfsVersionException(err);
        else if (errCode == ERR_CORRUPTED_FILE)
            throw new IgfsCorruptedFileException(err);
        else if (errCode == ERR_IGFS_GENERIC)
            throw new IgfsException(err);

        throw new IgniteCheckedException(err);
    }

    /**
     * @throws IgniteCheckedException Based on error code.
     */
    public void throwError() throws IgniteCheckedException {
        throwError(errCode, err);
    }

    /**
     * @return Error code.
     */
    public int errorCode() {
        return errCode;
    }

    /**
     * @param e Error if occurred.
     */
    public void error(IgniteCheckedException e) {
        err = e.getMessage();
        errCode = errorCode(e);
    }

    /**
     * @param streamId Stream ID.
     * @param err Error message if occurred.
     */
    public void error(long streamId, String err) {
        resType = RES_TYPE_ERR_STREAM_ID;

        res = streamId;
        errCode = ERR_GENERIC;

        this.err = err;
    }

    /**
     * Gets error code based on exception class.
     *
     * @param e Exception to analyze.
     * @return Error code.
     */
    private int errorCode(IgniteCheckedException e) {
        return errorCode(e, true);
    }

    /**
     * Gets error code based on exception class.
     *
     * @param e Exception to analyze.
     * @param checkIo Whether to check for IO exception.
     * @return Error code.
     */
    @SuppressWarnings("unchecked")
    private int errorCode(IgniteCheckedException e, boolean checkIo) {
        if (X.hasCause(e, IgfsPathNotFoundException.class))
            return ERR_FILE_NOT_FOUND;
        else if (e.hasCause(IgfsPathAlreadyExistsException.class))
            return ERR_PATH_ALREADY_EXISTS;
        else if (e.hasCause(IgfsDirectoryNotEmptyException.class))
            return ERR_DIRECTORY_NOT_EMPTY;
        else if (e.hasCause(IgfsParentNotDirectoryException.class))
            return ERR_PARENT_NOT_DIRECTORY;
        else if (e.hasCause(IgfsInvalidHdfsVersionException.class))
            return ERR_INVALID_HDFS_VERSION;
        else if (e.hasCause(IgfsCorruptedFileException.class))
            return ERR_CORRUPTED_FILE;
            // This check should be the last.
        else if (e.hasCause(IgfsException.class))
            return ERR_IGFS_GENERIC;

        return ERR_GENERIC;
    }

    /**
     * Writes object to data output. Do not use externalizable interface to avoid marshaller.
     *
     * @param out Data output.
     * @throws IOException If error occurred.
     */
    @SuppressWarnings("unchecked")
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] hdr = new byte[RES_HEADER_SIZE];

        U.intToBytes(resType, hdr, 0);

        int off = 4;

        hdr[off++] = err != null ? (byte)1 : (byte)0;

        if (resType == RES_TYPE_BYTE_ARRAY)
            U.intToBytes(len, hdr, off);

        out.write(hdr);

        if (err != null) {
            out.writeUTF(err);
            out.writeInt(errCode);

            if (resType == RES_TYPE_ERR_STREAM_ID)
                out.writeLong((Long)res);

            return;
        }

        switch (resType) {
            case RES_TYPE_BOOLEAN:
                out.writeBoolean((Boolean)res);

                break;

            case RES_TYPE_LONG:
                out.writeLong((Long)res);

                break;

            case RES_TYPE_BYTE_ARRAY:
                byte[][] buf = (byte[][])res;

                for (byte[] bytes : buf)
                    out.write(bytes);

                break;

            case RES_TYPE_IGFS_PATH:
            case RES_TYPE_IGFS_PATH_SUMMARY:
            case RES_TYPE_IGFS_FILE:
            case RES_TYPE_IGFS_STREAM_DESCRIPTOR:
            case RES_TYPE_HANDSHAKE:
            case RES_TYPE_STATUS: {
                out.writeBoolean(res != null);

                if (res != null)
                    ((Externalizable)res).writeExternal(out);

                break;
            }

            case RES_TYPE_COL_IGFS_FILE:
            case RES_TYPE_COL_IGFS_PATH:
            case RES_TYPE_COL_IGFS_BLOCK_LOCATION: {
                Collection<Externalizable> items = (Collection<Externalizable>)res;

                if (items != null) {
                    out.writeInt(items.size());

                    for (Externalizable item : items)
                        item.writeExternal(out);
                }
                else
                    out.writeInt(-1);

                break;
            }
        }
    }

    /**
     * Reads object from data input.
     *
     * @param in Data input.
     * @throws IOException If read failed.
     * @throws ClassNotFoundException If could not find class.
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] hdr = new byte[RES_HEADER_SIZE];

        in.readFully(hdr);

        resType = U.bytesToInt(hdr, 0);

        boolean hasErr = hdr[4] != 0;

        if (hasErr) {
            err = in.readUTF();
            errCode = in.readInt();

            if (resType == RES_TYPE_ERR_STREAM_ID)
                res = in.readLong();

            return;
        }

        switch (resType) {
            case RES_TYPE_BOOLEAN:
                res = in.readBoolean();

                break;

            case RES_TYPE_LONG:
                res = in.readLong();

                break;

            case RES_TYPE_IGFS_PATH: {
                boolean hasVal = in.readBoolean();

                if (hasVal) {
                    IgfsPath path = new IgfsPath();

                    path.readExternal(in);

                    res = path;
                }

                break;
            }

            case RES_TYPE_IGFS_PATH_SUMMARY: {
                boolean hasVal = in.readBoolean();

                if (hasVal) {
                    IgfsPathSummary sum = new IgfsPathSummary();

                    sum.readExternal(in);

                    res = sum;
                }

                break;
            }

            case RES_TYPE_IGFS_FILE: {
                boolean hasVal = in.readBoolean();

                if (hasVal) {
                    IgfsFileImpl file = new IgfsFileImpl();

                    file.readExternal(in);

                    res = file;
                }

                break;
            }

            case RES_TYPE_IGFS_STREAM_DESCRIPTOR: {
                boolean hasVal = in.readBoolean();

                if (hasVal) {
                    IgfsInputStreamDescriptor desc = new IgfsInputStreamDescriptor();

                    desc.readExternal(in);

                    res = desc;
                }

                break;
            }

            case RES_TYPE_HANDSHAKE: {
                boolean hasVal = in.readBoolean();

                if (hasVal) {
                    IgfsHandshakeResponse msg = new IgfsHandshakeResponse();

                    msg.readExternal(in);

                    res = msg;
                }

                break;
            }

            case RES_TYPE_STATUS: {
                boolean hasVal = in.readBoolean();

                if (hasVal) {
                    IgfsStatus msg = new IgfsStatus();

                    msg.readExternal(in);

                    res = msg;
                }

                break;
            }

            case RES_TYPE_COL_IGFS_FILE: {
                Collection<IgfsFile> files = null;

                int size = in.readInt();

                if (size >= 0) {
                    files = new ArrayList<>(size);

                    for (int i = 0; i < size; i++) {
                        IgfsFileImpl file = new IgfsFileImpl();

                        file.readExternal(in);

                        files.add(file);
                    }
                }

                res = files;

                break;
            }

            case RES_TYPE_COL_IGFS_PATH: {
                Collection<IgfsPath> paths = null;

                int size = in.readInt();

                if (size >= 0) {
                    paths = new ArrayList<>(size);

                    for (int i = 0; i < size; i++) {
                        IgfsPath path = new IgfsPath();

                        path.readExternal(in);

                        paths.add(path);
                    }
                }

                res = paths;

                break;
            }

            case RES_TYPE_COL_IGFS_BLOCK_LOCATION: {
                Collection<IgfsBlockLocation> locations = null;

                int size = in.readInt();

                if (size >= 0) {
                    locations = new ArrayList<>(size);

                    for (int i = 0; i < size; i++) {
                        IgfsBlockLocationImpl location = new IgfsBlockLocationImpl();

                        location.readExternal(in);

                        locations.add(location);
                    }
                }

                res = locations;

                break;
            }

            case RES_TYPE_BYTE_ARRAY:
                assert false : "Response type of byte array should never be processed by marshaller.";
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsControlResponse.class, this);
    }
}