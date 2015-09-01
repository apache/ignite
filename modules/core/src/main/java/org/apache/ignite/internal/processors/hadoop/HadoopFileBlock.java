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

package org.apache.ignite.internal.processors.hadoop;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.Arrays;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Hadoop file block.
 */
public class HadoopFileBlock extends HadoopInputSplit {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    protected URI file;

    /** */
    @GridToStringInclude
    protected long start;

    /** */
    @GridToStringInclude
    protected long len;

    /**
     * Creates new file block.
     */
    public HadoopFileBlock() {
        // No-op.
    }

    /**
     * Creates new file block.
     *
     * @param hosts List of hosts where the block resides.
     * @param file File URI.
     * @param start Start position of the block in the file.
     * @param len Length of the block.
     */
    public HadoopFileBlock(String[] hosts, URI file, long start, long len) {
        A.notNull(hosts, "hosts", file, "file");

        this.hosts = hosts;
        this.file = file;
        this.start = start;
        this.len = len;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(file());
        out.writeLong(start());
        out.writeLong(length());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        file = (URI)in.readObject();
        start = in.readLong();
        len = in.readLong();
    }

    /**
     * @return Length.
     */
    public long length() {
        return len;
    }

    /**
     * @param len New length.
     */
    public void length(long len) {
        this.len = len;
    }

    /**
     * @return Start.
     */
    public long start() {
        return start;
    }

    /**
     * @param start New start.
     */
    public void start(long start) {
        this.start = start;
    }

    /**
     * @return File.
     */
    public URI file() {
        return file;
    }

    /**
     * @param file New file.
     */
    public void file(URI file) {
        this.file = file;
    }

    /**
     * @param hosts New hosts.
     */
    public void hosts(String[] hosts) {
        A.notNull(hosts, "hosts");

        this.hosts = hosts;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof HadoopFileBlock))
            return false;

        HadoopFileBlock that = (HadoopFileBlock)o;

        return len == that.len && start == that.start && file.equals(that.file);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = file.hashCode();

        res = 31 * res + (int)(start ^ (start >>> 32));
        res = 31 * res + (int)(len ^ (len >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(HadoopFileBlock.class, this, "hosts", Arrays.toString(hosts));
    }
}