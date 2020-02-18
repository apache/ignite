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
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult.HIDDEN_DATA;

/**
 * Value container for result.
 */
public class PartitionReconciliationValueMeta extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Binary view. */
    @GridToStringInclude
    private byte[] binaryView;

    /** String view. */
    private String strView;

    /** Version. */
    private GridCacheVersion ver;

    /**
     * Default constructor for externalization.
     */
    public PartitionReconciliationValueMeta() {
    }

    /**
     *
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionReconciliationValueMeta(byte[] binaryView, String strView, GridCacheVersion ver) {
        this.binaryView = binaryView;
        this.strView = strView;
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeByteArray(out, binaryView);
        U.writeString(out, strView);
        out.writeObject(ver);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        binaryView = U.readByteArray(in);
        strView = U.readString(in);
        ver = (GridCacheVersion)in.readObject();
    }

    /**
     * @return string view.
     */
    public String stringView(boolean verbose) {
        return verbose ?
            strView + " hex=[" + U.byteArray2HexString(binaryView) + "]" + (ver != null ? " ver=[topVer=" +
                ver.topologyVersion() + ", order=" + ver.order() + ", nodeOrder=" + ver.nodeOrder() + ']' : "") :
            HIDDEN_DATA + (ver != null ? " ver=[topVer=" + ver.topologyVersion() + ", order=" + ver.order() +
                ", nodeOrder=" + ver.nodeOrder() : "") + ']';
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionReconciliationValueMeta meta = (PartitionReconciliationValueMeta)o;

        if (!Arrays.equals(binaryView, meta.binaryView))
            return false;
        if (!Objects.equals(strView, meta.strView))
            return false;
        return Objects.equals(ver, meta.ver);
    }
}
