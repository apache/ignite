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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for memory configuration.
 */
public class VisorMemoryPolicyConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique name of MemoryPolicy. */
    private String name;

    /** Page memory size in bytes. */
    private long size;

    /** Path for memory mapped file. */
    private String swapFilePath;


    /**
     * Default constructor.
     */
    public VisorMemoryPolicyConfiguration() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param plc Memory policy configuration.
     */
    public VisorMemoryPolicyConfiguration(MemoryPolicyConfiguration plc) {
        assert plc != null;

        name = plc.getName();
        size = plc.getSize();
        swapFilePath = plc.getSwapFilePath();
    }

    /**
     * Unique name of MemoryPolicy.
     */
    public String getName() {
        return name;
    }

    /**
     * Page memory size in bytes.
     */
    public long getSize() {
        return size;
    }

    /**
     * @return Path for memory mapped file.
     */
    public String getSwapFilePath() {
        return swapFilePath;
    }


    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        out.writeLong(size);
        U.writeString(out, swapFilePath);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        size = in.readLong();
        swapFilePath = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorMemoryPolicyConfiguration.class, this);
    }
}
