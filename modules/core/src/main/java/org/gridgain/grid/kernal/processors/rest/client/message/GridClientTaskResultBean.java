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

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.apache.ignite.portables.*;
import org.gridgain.grid.util.portable.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Task result.
 */
public class GridClientTaskResultBean implements Externalizable, PortableMarshalAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Synthetic ID containing task ID and result holding node ID. */
    private String id;

    /** Execution finished flag. */
    private boolean finished;

    /** Result. */
    private Object res;

    /** Error if any occurs while execution. */
    private String error;

    /**
     * @return Task ID.
     */
    public String getId() {
        return id;
    }

    /**
     * @param id Task ID.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return {@code true} if execution finished.
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * @param finished {@code true} if execution finished.
     */
    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    /**
     * @return Task result.
     */
    @SuppressWarnings("unchecked")
    public <R> R getResult() {
        return (R)res;
    }

    /**
     * @param res Task result.
     */
    public void setResult(Object res) {
        this.res = res;
    }

    /**
     * @return Error.
     */
    public String getError() {
        return error;
    }

    /**
     * @param error Error.
     */
    public void setError(String error) {
        this.error = error;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        PortableRawWriterEx raw = (PortableRawWriterEx)writer.rawWriter();

        raw.writeString(id);
        raw.writeBoolean(finished);

        raw.writeObject(res);

        raw.writeString(error);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        PortableRawReaderEx raw = (PortableRawReaderEx)reader.rawReader();

        id = raw.readString();
        finished = raw.readBoolean();

        res = raw.readObject();

        error = raw.readString();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, id);
        out.writeBoolean(finished);
        out.writeObject(res);
        U.writeString(out, error);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readString(in);
        finished = in.readBoolean();
        res = in.readObject();
        error = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [res=" + res + ", error=" + error +
            ", finished=" + finished + ", id=" + id + "]";
    }
}
