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

package org.apache.ignite.internal.processors.rest.client.message;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.portables.*;

import java.io.*;

/**
 * Request for a log file.
 */
public class GridClientLogRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Task name. */
    private String path;

    /** From line, inclusive, indexing from 0. */
    private int from = -1;

    /** To line, inclusive, indexing from 0, can exceed count of lines in log. */
    private int to = -1;

    /**
     * @return Path to log file.
     */
    public String path() {
        return path;
    }

    /**
     * @param path Path to log file.
     */
    public void path(String path) {
        this.path = path;
    }

    /**
     * @return From line, inclusive, indexing from 0.
     */
    public int from() {
        return from;
    }

    /**
     * @param from From line, inclusive, indexing from 0.
     */
    public void from(int from) {
        this.from = from;
    }

    /**
     * @return To line, inclusive, indexing from 0.
     */
    public int to() {
        return to;
    }

    /**
     * @param to To line, inclusive, indexing from 0.
     */
    public void to(int to) {
        this.to = to;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        super.writePortable(writer);

        PortableRawWriter raw = writer.rawWriter();

        raw.writeString(path);
        raw.writeInt(from);
        raw.writeInt(to);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        super.readPortable(reader);

        PortableRawReader raw = reader.rawReader();

        path = raw.readString();
        from = raw.readInt();
        to = raw.readInt();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeString(out, path);

        out.writeInt(from);
        out.writeInt(to);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        path = U.readString(in);

        from = in.readInt();
        to = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder b = new StringBuilder().
            append("GridClientLogRequest [path=").
            append(path);

        if (from != -1)
            b.append(", from=").append(from);

        if (to != -1)
            b.append(", to=").append(to);

        b.append(']');

        return b.toString();
    }
}
