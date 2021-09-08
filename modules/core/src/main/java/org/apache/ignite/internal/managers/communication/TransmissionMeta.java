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

package org.apache.ignite.internal.managers.communication;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Class represents a file meta information to send to the remote node. Used to initiate a new file transfer
 * process or to continue the previous unfinished from the last transmitted point.
 */
public class TransmissionMeta implements Externalizable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * The name to associate particular meta with.
     * Can be the particular file name, or an a transfer session identifier.
     */
    private String name;

    /** Offset of transferred file. */
    private long offset;

    /** Number of bytes to transfer started from given {@link #offset}. */
    private long cnt;

    /** Additional file params to transfer (e.g. partition id, partition name etc.). */
    private Map<String, Serializable> params;

    /** Read policy the way of how particular file will be handled. */
    private TransmissionPolicy plc;

    /** Last seen error if it has been occurred, or {@code null} the otherwise. */
    private Exception err;

    /** Default constructor, usually used to create meta to read channel data into. */
    public TransmissionMeta() {
        this(null);
    }

    /**
     * @param err Last seen error if it has been occurred, or {@code null} the otherwise.
     */
    public TransmissionMeta(Exception err) {
        this("", -1, -1, null, null, err);
    }

    /**
     * @param name File name to assoticate particular meta with.
     * @param offset The start position of file.
     * @param cnt Number of bytes expected to transfer.
     * @param params Additional meta params.
     * @param plc Policy of how file will be handled.
     * @param err Last seen error if it has been occurred, or {@code null} the otherwise.
     */
    public TransmissionMeta(
        String name,
        long offset,
        long cnt,
        Map<String, Serializable> params,
        TransmissionPolicy plc,
        Exception err
    ) {
        assert params instanceof Serializable || params == null : params.getClass();

        this.name = name;
        this.offset = offset;
        this.cnt = cnt;
        this.params = params;
        this.plc = plc;
        this.err = err;
    }

    /**
     * @return File name.
     */
    public String name() {
        assert name != null;

        return name;
    }

    /**
     * @return Position to start channel transfer at.
     */
    public long offset() {
        return offset;
    }

    /**
     * @return Number of bytes expected to transfer.
     */
    public long count() {
        return cnt;
    }

    /**
     * @return Additional params.
     */
    public Map<String, Serializable> params() {
        return params;
    }

    /**
     * @return Transmission policy.
     */
    public TransmissionPolicy policy() {
        return plc;
    }

    /**
     * @param err An exception instance if it has been previously occurred.
     * @return {@code This} for chaining.
     */
    public TransmissionMeta error(Exception err) {
        this.err = err;

        return this;
    }

    /**
     * @return An exception instance if it has been previously occurred.
     */
    public Exception error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(offset);
        out.writeLong(cnt);
        out.writeObject(params);
        out.writeObject(plc);
        out.writeObject(err);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
        offset = in.readLong();
        cnt = in.readLong();
        params = (Map)in.readObject();
        plc = (TransmissionPolicy)in.readObject();
        err = (Exception)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TransmissionMeta meta = (TransmissionMeta)o;

        return offset == meta.offset &&
            cnt == meta.cnt &&
            name.equals(meta.name) &&
            Objects.equals(params, meta.params) &&
            plc == meta.plc &&
            Objects.equals(err, meta.err);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, offset, cnt, params, plc, err);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransmissionMeta.class, this);
    }
}
