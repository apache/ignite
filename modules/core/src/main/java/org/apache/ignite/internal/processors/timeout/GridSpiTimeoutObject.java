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

package org.apache.ignite.internal.processors.timeout;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;

/**
 * Wrapper for {@link IgniteSpiTimeoutObject}.
 */
public class GridSpiTimeoutObject implements GridTimeoutObject {
    /** */
    @GridToStringInclude
    private final IgniteSpiTimeoutObject obj;

    /**
     * @param obj SPI object.
     */
    public GridSpiTimeoutObject(IgniteSpiTimeoutObject obj) {
        this.obj = obj;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        obj.onTimeout();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        return obj.id();
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return obj.endTime();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        assert false;

        return super.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        assert false;

        return super.equals(obj);
    }

    /** {@inheritDoc} */
    @Override public final String toString() {
        return S.toString(GridSpiTimeoutObject.class, this);
    }
}