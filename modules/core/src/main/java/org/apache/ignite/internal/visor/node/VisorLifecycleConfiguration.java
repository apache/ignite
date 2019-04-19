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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactArray;

/**
 * Data transfer object for node lifecycle configuration properties.
 */
public class VisorLifecycleConfiguration extends VisorDataTransferObject{
    /** */
    private static final long serialVersionUID = 0L;

    /** Lifecycle beans. */
    private String beans;

    /**
     * Default constructor.
     */
    public VisorLifecycleConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for node lifecycle configuration properties.
     *
     * @param c Grid configuration.
     */
    public VisorLifecycleConfiguration(IgniteConfiguration c) {
        beans = compactArray(c.getLifecycleBeans());
    }

    /**
     * @return Lifecycle beans.
     */
    @Nullable public String getBeans() {
        return beans;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, beans);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        beans = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLifecycleConfiguration.class, this);
    }
}
