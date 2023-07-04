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

package org.apache.ignite.internal.management.kill;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.management.kill.SnapshotCancelTask.CancelSnapshotArg;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class KillSnapshotCommandArg extends CancelSnapshotArg {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(description = "Request id")
    private UUID requestId;

    /** */
    private String snapshotName;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, requestId);
        U.writeString(out, snapshotName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        requestId = U.readUuid(in);
        snapshotName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public UUID requestId() {
        return requestId;
    }

    /** */
    public void requestId(UUID requestId) {
        this.requestId = requestId;
    }

    /** {@inheritDoc} */
    @Override public String snapshotName() {
        return snapshotName;
    }

    /** */
    public void snapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }
}
