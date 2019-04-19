/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.checkpoint;

import java.io.Serializable;

/**
 * Grid checkpoint test state
 */
public class GridCheckpointTestState implements Serializable {
    /** */
    private String data;

    /**
     * @param data Data.
     */
    public GridCheckpointTestState(String data) {
        this.data = data;
    }

    /**
     * Gets data.
     *
     * @return data.
     */
    public String getData() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return !(obj == null || !obj.getClass().equals(getClass())) && ((GridCheckpointTestState)obj).data.equals(data);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return data.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(getClass().getSimpleName());
        buf.append(" [data='").append(data).append('\'');
        buf.append(']');

        return buf.toString();
    }
}