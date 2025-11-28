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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

/** Node status information for rolling upgrade. */
public class RollingUpgradeStatusNode extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Object consistentId;

    /** */
    private String address;

    /** */
    private IgniteProductVersion ver;

    /** */
    public RollingUpgradeStatusNode() {
        // No-op.
    }

    /** */
    public RollingUpgradeStatusNode(Object consistentId, String address, IgniteProductVersion ver) {
        this.consistentId = consistentId;
        this.address = address;
        this.ver = ver;
    }

    /** */
    public Object consistentId() {
        return consistentId;
    }

    /** */
    public IgniteProductVersion version() {
        return ver;
    }

    /** */
    public String address() {
        return address;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(consistentId);
        U.writeString(out, address);
        out.writeObject(ver);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        consistentId = in.readObject();
        address = U.readString(in);
        ver = (IgniteProductVersion)in.readObject();
    }
}
