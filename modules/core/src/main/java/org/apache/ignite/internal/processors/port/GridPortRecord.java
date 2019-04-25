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

package org.apache.ignite.internal.processors.port;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.IgnitePortProtocol;

/**
 * This class defines record about port use.
 */
public class GridPortRecord {
    /** Port. */
    private int port;

    /** Protocol. */
    private IgnitePortProtocol proto;

    /** Class which uses port. */
    private Class cls;

    /**
     * @param port Port.
     * @param proto Protocol.
     * @param cls Class.
     */
    GridPortRecord(int port, IgnitePortProtocol proto, Class cls) {
        this.port = port;
        this.proto = proto;
        this.cls = cls;
    }

    /**
     * @return Port.
     */
    public int port() {
        return port;
    }

    /**
     * @return Protocol.
     */
    public IgnitePortProtocol protocol() {
        return proto;
    }

    /**
     * @return Class.
     */
    public Class clazz() {
        return cls;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPortRecord.class, this);
    }
}