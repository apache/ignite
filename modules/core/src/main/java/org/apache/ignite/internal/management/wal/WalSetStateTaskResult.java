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

package org.apache.ignite.internal.management.wal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result of WAL enable/disable operation.
 */
public class WalSetStateTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Success flag. */
    private Boolean success;

    /** Successfully processed groups. */
    private List<String> successGrps;

    /** Error messages if operation failed. */
    private List<String> errMsgs;

    /** Default constructor. */
    public WalSetStateTaskResult() {
        // No-op.
    }

    /**
     * Constructor for success.
     *
     * @param successGrps Successfully processed groups.
     */
    public WalSetStateTaskResult(List<String> successGrps) {
        this.success = true;
        this.successGrps = new ArrayList<>(successGrps);
        this.errMsgs = null;
    }

    /**
     * Constructor for failure.
     *
     * @param successGrps Successfully processed groups.
     * @param errMsgs Error messages.
     */
    public WalSetStateTaskResult(List<String> successGrps, List<String> errMsgs) {
        this.success = false;
        this.successGrps = new ArrayList<>(successGrps);
        this.errMsgs = new ArrayList<>(errMsgs);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(success);
        U.writeCollection(out, successGrps);
        U.writeCollection(out, errMsgs);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        success = (Boolean)in.readObject();
        successGrps = U.readList(in);
        errMsgs = U.readList(in);
    }

    /**
     * @return Success flag.
     */
    public Boolean success() {
        return success;
    }

    /**
     * @return Successfully processed groups.
     */
    public List<String> successGroups() {
        return successGrps;
    }

    /**
     * @return Error messages if operation failed.
     */
    public List<String> errorMessages() {
        return errMsgs;
    }
}
