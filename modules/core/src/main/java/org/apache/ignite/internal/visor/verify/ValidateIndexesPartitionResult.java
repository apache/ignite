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

package org.apache.ignite.internal.visor.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Encapsulates intermediate results of validation of SQL index (if {@link #sqlIdxName} is present) or partition.
 */
public class ValidateIndexesPartitionResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Update counter. */
    private long updateCntr;

    /** Size. */
    private long size;

    /** Is primary. */
    private boolean isPrimary;

    /** Consistent id. */
    @GridToStringInclude
    private Object consistentId;

    /** Issues. */
    @GridToStringExclude
    private List<IndexValidationIssue> issues = new ArrayList<>(10);

    /** Sql index name. */
    @GridToStringExclude
    private String sqlIdxName;

    /**
     *
     */
    public ValidateIndexesPartitionResult() {
        // Empty constructor required for Externalizable.
    }

    /**
     * @param updateCntr Update counter.
     * @param size Size.
     * @param isPrimary Is primary.
     * @param consistentId Consistent id.
     * @param sqlIdxName Sql index name (optional).
     */
    public ValidateIndexesPartitionResult(long updateCntr, long size, boolean isPrimary, Object consistentId,
        String sqlIdxName) {
        this.updateCntr = updateCntr;
        this.size = size;
        this.isPrimary = isPrimary;
        this.consistentId = consistentId;
        this.sqlIdxName = sqlIdxName;
    }

    /**
     *
     */
    public long updateCntr() {
        return updateCntr;
    }

    /**
     *
     */
    public long size() {
        return size;
    }

    /**
     *
     */
    public boolean primary() {
        return isPrimary;
    }

    /**
     *
     */
    public Object consistentId() {
        return consistentId;
    }

    /**
     *
     */
    public List<IndexValidationIssue> issues() {
        return issues;
    }

    /**
     * @return <code>null</code> for partition validation result, SQL index name for index validation result
     */
    public String sqlIndexName() {
        return sqlIdxName;
    }

    /**
     * @param t Issue.
     * @return True if there are already enough issues.
     */
    public boolean reportIssue(IndexValidationIssue t) {
        if (issues.size() >= 10)
            return true;

        issues.add(t);

        return false;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(updateCntr);
        out.writeLong(size);
        out.writeBoolean(isPrimary);
        out.writeObject(consistentId);
        U.writeCollection(out, issues);
        U.writeString(out, sqlIdxName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        updateCntr = in.readLong();
        size = in.readLong();
        isPrimary = in.readBoolean();
        consistentId = in.readObject();
        issues = U.readList(in);

        if (protoVer >= V2)
            sqlIdxName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return sqlIdxName == null ? S.toString(ValidateIndexesPartitionResult.class, this) :
            ValidateIndexesPartitionResult.class.getSimpleName() + " [consistentId=" + consistentId +
                ", sqlIdxName=" + sqlIdxName + "]";
    }
}
