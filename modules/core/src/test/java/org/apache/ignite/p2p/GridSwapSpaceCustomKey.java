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

package org.apache.ignite.p2p;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 */
public class GridSwapSpaceCustomKey implements Serializable {
    /** */
    private long id = -1;

    /**
     * @return ID.
     */
    public long getId() {
        return id;
    }

    /**
     *
     * @param id ID.
     */
    public void setId(long id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj instanceof GridSwapSpaceCustomKey && ((GridSwapSpaceCustomKey)obj).id == id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Long.valueOf(id).hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSwapSpaceCustomKey.class, this);
    }
}
