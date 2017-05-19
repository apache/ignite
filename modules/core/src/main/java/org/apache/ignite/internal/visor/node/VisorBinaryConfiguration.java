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
import java.util.List;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for configuration of binary data structures.
 */
public class VisorBinaryConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID mapper. */
    private String idMapper;

    /** Name mapper. */
    private String nameMapper;

    /** Serializer. */
    private String serializer;

    /** Types. */
    private List<VisorBinaryTypeConfiguration> typeCfgs;

    /** Compact footer flag. */
    private boolean compactFooter;

    /**
     * Default constructor.
     */
    public VisorBinaryConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for binary configuration.
     *
     * @param src Binary configuration.
     */
    public VisorBinaryConfiguration(BinaryConfiguration src) {
        idMapper = compactClass(src.getIdMapper());
        nameMapper = compactClass(src.getNameMapper());
        serializer = compactClass(src.getSerializer());
        compactFooter = src.isCompactFooter();

        typeCfgs = VisorBinaryTypeConfiguration.list(src.getTypeConfigurations());
    }

    /**
     * @return ID mapper.
     */
    public String getIdMapper() {
        return idMapper;
    }

    /**
     * @return Name mapper.
     */
    public String getNameMapper() {
        return nameMapper;
    }

    /**
     * @return Serializer.
     */
    public String getSerializer() {
        return serializer;
    }

    /**
     * @return Types.
     */
    public List<VisorBinaryTypeConfiguration> getTypeConfigurations() {
        return typeCfgs;
    }

    /**
     * @return Compact footer flag.
     */
    public boolean isCompactFooter() {
        return compactFooter;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, idMapper);
        U.writeString(out, nameMapper);
        U.writeString(out, serializer);
        U.writeCollection(out, typeCfgs);
        out.writeBoolean(compactFooter);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        idMapper = U.readString(in);
        nameMapper = U.readString(in);
        serializer = U.readString(in);
        typeCfgs = U.readList(in);
        compactFooter = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBinaryConfiguration.class, this);
    }
}
