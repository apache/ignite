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

package org.apache.ignite.internal.management.rollingupgrade;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

/** */
public class RollingUpgradeTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteProductVersion curVer;

    /** */
    private IgniteProductVersion targetVer;

    /** */
    private String errMsg;

    /** */
    public RollingUpgradeTaskResult(IgniteProductVersion curVer, IgniteProductVersion targetVer, String errMsg) {
        this.curVer = curVer;
        this.targetVer = targetVer;
        this.errMsg = errMsg;
    }

    /** */
    public RollingUpgradeTaskResult() {
        // No-op.
    }

    /** */
    public String errorMessage() {
        return errMsg;
    }

    /** */
    public IgniteProductVersion currentVersion() {
        return curVer;
    }

    /** */
    public void currentVersion(IgniteProductVersion curVer) {
        this.curVer = curVer;
    }

    /** */
    public IgniteProductVersion targetVersion() {
        return targetVer;
    }

    /** */
    public void targetVersion(IgniteProductVersion targetVer) {
        this.targetVer = targetVer;
    }

    /** */
    public void errorMessage(String errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(curVer);
        out.writeObject(targetVer);
        U.writeString(out,errMsg);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        this.curVer = (IgniteProductVersion)in.readObject();
        this.targetVer = (IgniteProductVersion)in.readObject();
        this.errMsg = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RollingUpgradeTaskResult.class, this);
    }
}
