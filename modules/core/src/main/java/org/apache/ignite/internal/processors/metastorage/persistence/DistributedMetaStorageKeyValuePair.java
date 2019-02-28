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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
@SuppressWarnings("PublicField")
final class DistributedMetaStorageKeyValuePair implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final DistributedMetaStorageKeyValuePair[] EMPTY_ARRAY = {};

    /** */
    @GridToStringInclude
    public final String key;

    /** */
    @GridToStringInclude
    public final byte[] valBytes;

    /** */
    public DistributedMetaStorageKeyValuePair(String key, byte[] valBytes) {
        this.key = key;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        DistributedMetaStorageKeyValuePair pair = (DistributedMetaStorageKeyValuePair)o;

        return key.equals(pair.key) && Arrays.equals(valBytes, pair.valBytes);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * key.hashCode() + Arrays.hashCode(valBytes);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedMetaStorageKeyValuePair.class, this);
    }
}
