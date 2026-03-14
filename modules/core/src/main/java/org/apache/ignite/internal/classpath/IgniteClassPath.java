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
    public IgniteClassPath(UUID id, String name, String[] files, long[] lengths) {
        this.id = id;
        this.name = name;
        this.files = files;
        this.lengths = lengths;
        this.state = IgniteClassPathState.CREATING;
    }

    /** */
    public IgniteClassPathState state() {
        return state;
    }

    /** */
    public void state(IgniteClassPathState state) {
        this.state = state;
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
}
