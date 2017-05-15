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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for configuration of executor data structures.
 */
public class VisorExecutorConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Thread pool name. */
    private String name;

    /** Thread pool size. */
    private int size;

    /**
     * Construct data transfer object for Executor configurations properties.
     *
     * @param cfgs Executor configurations.
     * @return Executor configurations properties.
     */
    public static List<VisorExecutorConfiguration> list(ExecutorConfiguration[] cfgs) {
        List<VisorExecutorConfiguration> res = new ArrayList<>();

        if (!F.isEmpty(cfgs)) {
            for (ExecutorConfiguration cfg : cfgs)
                res.add(new VisorExecutorConfiguration(cfg));
        }

        return res;
    }

    /**
     * Default constructor.
     */
    public VisorExecutorConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for executor configuration.
     *
     * @param src Executor configuration.
     */
    public VisorExecutorConfiguration(ExecutorConfiguration src) {
        name = src.getName();
        size = src.getSize();
    }

    /**
     * @return Executor name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Thread pool size.
     */
    public int getSize() {
        return size;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        out.writeInt(size);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        size = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorExecutorConfiguration.class, this);
    }
}
