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

package org.apache.ignite.internal.visor.igfs;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result for {@link VisorIgfsProfilerClearTask}.
 */
@Deprecated
public class VisorIgfsProfilerClearTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Count of deleted files. */
    private int deleted;

    /** Count of not deleted files. */
    private int notDeleted;

    /**
     * Default constructor.
     */
    public VisorIgfsProfilerClearTaskResult() {
        // No-op.
    }

    /**
     * @param deleted Count of deleted files.
     * @param notDeleted Count of not deleted files.
     */
    public VisorIgfsProfilerClearTaskResult(int deleted, int notDeleted) {
        this.deleted = deleted;
        this.notDeleted = notDeleted;
    }

    /**
     * @return Count of deleted files.
     */
    public Integer getDeleted() {
        return deleted;
    }

    /**
     * @return Count of not deleted files.
     */
    public Integer getNotDeleted() {
        return notDeleted;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(deleted);
        out.writeInt(notDeleted);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        deleted = in.readInt();
        notDeleted = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfsProfilerClearTaskResult.class, this);
    }
}
