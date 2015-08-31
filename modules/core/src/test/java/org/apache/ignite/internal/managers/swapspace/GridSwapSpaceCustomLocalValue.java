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

package org.apache.ignite.internal.managers.swapspace;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 */
public class GridSwapSpaceCustomLocalValue implements Serializable {
    /** */
    private long id = -1;

    /** */
    private String data;

    /**
     *
     */
    public GridSwapSpaceCustomLocalValue() {
        /* No-op. */
    }

    /**
     *
     * @param id Id.
     * @param data Data.
     */
    public GridSwapSpaceCustomLocalValue(long id, String data) {
        this.id = id;
        this.data = data;
    }

    /**
     *
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

    /**
     *
     * @return Data.
     */
    public String getData() {
        return data;
    }

    /**
     *
     * @param data Data.
     */
    public void setData(String data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSwapSpaceCustomLocalValue.class, this);
    }
}