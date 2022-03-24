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

package org.apache.ignite.internal.visor.consistency;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class VisorConsistencyRepairTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Partition. */
    private int part;

    /** Strategy. */
    ReadRepairStrategy strategy;

    /**
     * Default constructor.
     */
    public VisorConsistencyRepairTaskArg() {
    }

    /**
     * @param cacheName Cache name.
     * @param part Part.
     */
    public VisorConsistencyRepairTaskArg(String cacheName, int part, ReadRepairStrategy strategy) {
        this.cacheName = cacheName;
        this.part = part;
        this.strategy = strategy;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        out.writeInt(part);
        U.writeEnum(out, strategy);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        part = in.readInt();
        strategy = U.readEnum(in, ReadRepairStrategy.class);
    }

    /**
     *
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     *
     */
    public int part() {
        return part;
    }

    /**
     *
     */
    public ReadRepairStrategy strategy() {
        return strategy;
    }
}
