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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Implementation of {@link Storable} which holds byte array of variable length.
 */
public class SimpleDataRow implements Storable {
    /** */
    private long link;

    /** */
    private final int part;

    /** */
    @GridToStringExclude
    private final byte[] val;

    /**
     * @param link Link.
     * @param part Partition.
     * @param val Value.
     */
    public SimpleDataRow(long link, int part, byte[] val) {
        this.link = link;
        this.part = part;
        this.val = val;
    }

    /**
     * @param part Partition.
     * @param val Value.
     */
    public SimpleDataRow(int part, byte[] val) {
        this.part = part;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public void link(long link) {
        this.link = link;
    }

    /** {@inheritDoc} */
    @Override public long link() {
        return link;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public int size() throws IgniteCheckedException {
        return 2 /** Fragment size */ + 2 /** Row size */ + value().length;
    }

    /** {@inheritDoc} */
    @Override public int headerSize() {
        return 0;
    }

    /**
     * @return Value.
     */
    public byte[] value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public IOVersions<? extends AbstractDataPageIO> ioVersions() {
        return SimpleDataPageIO.VERSIONS;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SimpleDataRow.class, this, "len", val.length);
    }
}
