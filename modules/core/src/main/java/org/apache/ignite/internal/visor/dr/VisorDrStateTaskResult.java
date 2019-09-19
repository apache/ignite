/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class VisorDrStateTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Data center id. */
    private byte dataCenterId;
    /** Result messages. */
    private List<String> resultMessages = new ArrayList<>();

    /** */
    public void setDataCenterId(byte dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    /** */
    public byte getDataCenterId() {
        return dataCenterId;
    }

    /** */
    public List<String> getResultMessages() {
        return resultMessages;
    }

    /** */
    public void addResultMessage(String msg) {
        resultMessages.add(msg);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeByte(dataCenterId);
        U.writeCollection(out, resultMessages);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        dataCenterId = in.readByte();
        resultMessages = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrStateTaskResult.class, this);
    }
}
