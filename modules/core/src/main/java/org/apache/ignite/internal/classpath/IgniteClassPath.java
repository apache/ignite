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

package org.apache.ignite.internal.classpath;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Class path POJO.
 */
public class IgniteClassPath implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID id;

    /** */
    private Object uploadNodeConsistentId;

    /** */
    private String name;

    /** */
    private String[] files;

    /** */
    private long[] lengths;

    /** */
    private IgniteClassPathState state;

    /**
     * @param id Unique id of classpath.
     * @param name User provided name.
     * @param files Files to include to classpath.
     */
    public IgniteClassPath(
        UUID id,
        Object uploadNodeConsistentId,
        String name,
        String[] files,
        long[] lengths,
        IgniteClassPathState state
    ) {
        this.id = id;
        this.name = name;
        this.files = files;
        this.lengths = lengths;
        this.state = state;
    }

    /**
     * @param state New state.
     */
    IgniteClassPath newState(IgniteClassPathState state) {
        return new IgniteClassPath(id, uploadNodeConsistentId, name, files, lengths, state);
    }

    /** @return Consistent id of the node that starts ICP creation. */
    public Object uploadNodeConsistentId() {
        return uploadNodeConsistentId;
    }

    /** */
    public IgniteClassPathState state() {
        return state;
    }

    /** */
    public UUID id() {
        return id;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public String[] files() {
        return files;
    }

    /** */
    public long[] lengths() {
        return lengths;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteClassPath.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        IgniteClassPath that = (IgniteClassPath) o;
        return Objects.equals(id, that.id) && Objects.equals(uploadNodeConsistentId, that.uploadNodeConsistentId)
            && Objects.equals(name, that.name) && Objects.deepEquals(files, that.files)
            && Objects.deepEquals(lengths, that.lengths) && state == that.state;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(id, uploadNodeConsistentId, name, Arrays.hashCode(files), Arrays.hashCode(lengths), state);
    }
}
