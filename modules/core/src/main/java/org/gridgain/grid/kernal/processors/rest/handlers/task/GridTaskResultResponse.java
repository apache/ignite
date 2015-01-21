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

package org.gridgain.grid.kernal.processors.rest.handlers.task;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Task result response.
 */
public class GridTaskResultResponse extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Result. */
    @GridDirectTransient
    private Object res;

    /** Serialized result. */
    private byte[] resBytes;

    /** Finished flag. */
    private boolean finished;

    /** Flag indicating that task has ever been launched on node. */
    private boolean found;

    /** Error. */
    private String err;

    /**
     * @return Task result.
     */
    @Nullable public Object result() {
        return res;
    }

    /**
     * @param res Task result.
     */
    public void result(@Nullable Object res) {
        this.res = res;
    }

    /**
     * @param resBytes Serialized result.
     */
    public void resultBytes(byte[] resBytes) {
        this.resBytes = resBytes;
    }

    /**
     * @return Serialized result.
     */
    public byte[] resultBytes() {
        return resBytes;
    }

    /**
     * @return {@code true} if finished.
     */
    public boolean finished() {
        return finished;
    }

    /**
     * @param finished {@code true} if finished.
     */
    public void finished(boolean finished) {
        this.finished = finished;
    }

    /**
     * @return {@code true} if found.
     */
    public boolean found() {
        return found;
    }

    /**
     * @param found {@code true} if found.
     */
    public void found(boolean found) {
        this.found = found;
    }

    /**
     * @return Error.
     */
    public String error() {
        return err;
    }

    /**
     * @param err Error.
     */
    public void error(String err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridTaskResultResponse _clone = new GridTaskResultResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridTaskResultResponse _clone = (GridTaskResultResponse)_msg;

        _clone.res = res;
        _clone.resBytes = resBytes;
        _clone.finished = finished;
        _clone.found = found;
        _clone.err = err;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putString(err))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putBoolean(finished))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putBoolean(found))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putByteArray(resBytes))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                String err0 = commState.getString();

                if (err0 == STR_NOT_READ)
                    return false;

                err = err0;

                commState.idx++;

            case 1:
                if (buf.remaining() < 1)
                    return false;

                finished = commState.getBoolean();

                commState.idx++;

            case 2:
                if (buf.remaining() < 1)
                    return false;

                found = commState.getBoolean();

                commState.idx++;

            case 3:
                byte[] resBytes0 = commState.getByteArray();

                if (resBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                resBytes = resBytes0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 74;
    }
}
