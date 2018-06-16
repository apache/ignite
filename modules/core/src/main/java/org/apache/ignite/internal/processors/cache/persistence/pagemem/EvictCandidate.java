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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class EvictCandidate {
    /** */
    private int tag;

    /** */
    @GridToStringExclude
    private long relPtr;

    /** */
    @GridToStringInclude
    private FullPageId fullId;

    /**
     * @param tag Tag.
     * @param relPtr Relative pointer.
     * @param fullId Full page ID.
     */
    public EvictCandidate(int tag, long relPtr, FullPageId fullId) {
        this.tag = tag;
        this.relPtr = relPtr;
        this.fullId = fullId;
    }

    /**
     * @return Tag.
     */
    public int tag() {
        return tag;
    }

    /**
     * @return Relative pointer.
     */
    public long relativePointer() {
        return relPtr;
    }

    /**
     * @return Index.
     */
    public FullPageId fullId() {
        return fullId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(EvictCandidate.class, this, "relPtr", U.hexLong(relPtr));
    }
}
