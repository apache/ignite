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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public abstract class SimpleDataRow implements Storable {
    /** */
    private long link;

    /** */
    private final int part;

    /** */
    @GridToStringExclude
    private final byte[] val;

    public SimpleDataRow(long link, int part, byte[] val) {
        this.link = link;
        this.part = part;
        this.val = val;
    }

    public SimpleDataRow(int part, byte[] val) {
        this.part = part;
        this.val = val;
    }

    @Override public void link(long link) {
        this.link = link;
    }

    @Override public long link() {
        return link;
    }

    @Override public int partition() {
        return part;
    }

    @Override public int size() throws IgniteCheckedException {
        return 2 /** Fragment size */ + 2 /** Row size */ + value().length;
    }

    @Override public int headerSize() {
        return 0;
    }

    public byte[] value() {
        return val;
    }

    @Override public String toString() {
        return S.toString(SimpleDataRow.class, this, "len", val.length);
    }
}
