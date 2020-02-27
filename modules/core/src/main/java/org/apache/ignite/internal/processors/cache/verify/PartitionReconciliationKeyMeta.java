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

package org.apache.ignite.internal.processors.cache.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries.HIDDEN_DATA;

/**
 * Container for key value.
 */
public class PartitionReconciliationKeyMeta extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Binary view. */
    @GridToStringInclude
    private byte[] binaryView;

    /** String view. */
    private String strView;

    /**
     * Default constructor for externalization.
     */
    public PartitionReconciliationKeyMeta() {
    }

    /**
     * @param binaryView Binary view.
     * @param strView String view.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionReconciliationKeyMeta(byte[] binaryView, String strView) {
        this.binaryView = binaryView;
        this.strView = strView;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeByteArray(out, binaryView);
        U.writeString(out, strView);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException,
        ClassNotFoundException {
        binaryView = U.readByteArray(in);
        strView = U.readString(in);
    }

    /**
     * @return string view.
     */
    public String stringView(boolean verbose) {
        return verbose ? strView + " hex=[" + U.byteArray2HexString(binaryView) + ']' : HIDDEN_DATA;
    }

    /**
     * @return Binary view.
     */
    public byte[] binaryView() {
        return binaryView;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionReconciliationKeyMeta meta = (PartitionReconciliationKeyMeta)o;

        if (!Arrays.equals(binaryView, meta.binaryView))
            return false;

        return Objects.equals(strView, meta.strView);
    }
}
