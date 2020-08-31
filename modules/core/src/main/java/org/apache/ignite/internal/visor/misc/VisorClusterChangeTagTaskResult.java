/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.misc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class VisorClusterChangeTagTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String tag;

    /** */
    private Boolean success;

    /** */
    private String errResp;

    /** Default constructor. */
    public VisorClusterChangeTagTaskResult() {
        // No-op.
    }

    /**
     * @param tag Cluster tag.
     * @param success Success of update tag operation.
     * @param errResp Error response returned if cluster tag update has failed.
     */
    public VisorClusterChangeTagTaskResult(String tag, @Nullable Boolean success, @Nullable String errResp) {
        this.tag = tag;
        this.success = success;
        this.errResp = errResp;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(success);
        out.writeObject(errResp);

        out.writeObject(tag);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        success = (Boolean)in.readObject();
        errResp = (String)in.readObject();

        tag = (String)in.readObject();
    }

    /** */
    public String tag() {
        return tag;
    }

    /** */
    public Boolean success() {
        return success;
    }

    /** */
    public String errorMessage() {
        return errResp;
    }
}
