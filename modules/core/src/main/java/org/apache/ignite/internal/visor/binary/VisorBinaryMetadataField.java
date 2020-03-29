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

package org.apache.ignite.internal.visor.binary;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Binary object metadata field information.
 */
public class VisorBinaryMetadataField extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field name. */
    private String fieldName;

    /** Field type name. */
    private String fieldTypeName;

    /** Field id. */
    private Integer fieldId;

    /**
     * Default constructor.
     */
    public VisorBinaryMetadataField() {
        // No-op.
    }

    /**
     * @param fieldName Field name.
     * @param fieldTypeName Field type name.
     * @param fieldId Field id.
     */
    public VisorBinaryMetadataField(String fieldName, String fieldTypeName, Integer fieldId) {
        this.fieldName = fieldName;
        this.fieldTypeName = fieldTypeName;
        this.fieldId = fieldId;
    }

    /**
     * @return Field name.
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * @return Field type name.
     */
    @Nullable public String getFieldTypeName() {
        return fieldTypeName;
    }

    /**
     * @return Field id.
     */
    public Integer getFieldId() {
        return fieldId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, fieldName);
        U.writeString(out, fieldTypeName);
        out.writeObject(fieldId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        fieldName = U.readString(in);
        fieldTypeName = U.readString(in);
        fieldId = (Integer)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBinaryMetadataField.class, this);
    }
}
