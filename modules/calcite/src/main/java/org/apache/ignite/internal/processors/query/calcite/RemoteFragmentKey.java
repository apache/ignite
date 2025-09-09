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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.UUID;

/** */
final class RemoteFragmentKey {
    /** */
    private final UUID nodeId;

    /** */
    private final long fragmentId;

    /** */
    RemoteFragmentKey(UUID nodeId, long fragmentId) {
        this.nodeId = nodeId;
        this.fragmentId = fragmentId;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public long fragmentId() {
        return fragmentId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RemoteFragmentKey that = (RemoteFragmentKey)o;

        if (fragmentId != that.fragmentId)
            return false;
        return nodeId.equals(that.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId.hashCode();
        res = 31 * res + (int)(fragmentId ^ (fragmentId >>> 32));
        return res;
    }
}
