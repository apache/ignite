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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.readString;
import static org.apache.ignite.internal.util.IgniteUtils.writeString;

/**
 * Issue when checking size of cache and index.
 */
public class ValidateIndexesCheckSizeIssue extends VisorDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Index name. */
    private String idxName;

    /** Index size. */
    private long idxSize;

    /** Error. */
    @GridToStringExclude
    private Throwable t;

    /**
     * Default constructor.
     */
    public ValidateIndexesCheckSizeIssue() {
        //Default constructor required for Externalizable.
    }

    /**
     * Constructor.
     *
     * @param idxName    Index name.
     * @param idxSize    Index size.
     * @param t          Error.
     */
    public ValidateIndexesCheckSizeIssue(@Nullable String idxName, long idxSize, @Nullable Throwable t) {
        this.idxName = idxName;
        this.idxSize = idxSize;
        this.t = t;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        writeString(out, idxName);
        out.writeLong(idxSize);
        out.writeObject(t);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        idxName = readString(in);
        idxSize = in.readLong();
        t = (Throwable)in.readObject();
    }

    /**
     * Return index size.
     *
     * @return Index size.
     */
    public long idxSize() {
        return idxSize;
    }

    /**
     * Return error.
     *
     * @return Error.
     */
    public Throwable err() {
        return t;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ValidateIndexesCheckSizeIssue.class, this) + ", " + t.getClass() + ": " + t.getMessage();
    }
}
