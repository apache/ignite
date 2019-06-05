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

package org.apache.ignite.internal.visor.diagnostic;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/**
 *
 */
public class VisorPageLocksResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String payload;

    /**
     *
     */
    public VisorPageLocksResult() {
        //No-op.
    }

    /**
     * @param payload Result payload as string.
     */
    public VisorPageLocksResult(String payload) {
        this.payload = payload;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        if (payload == null) {
            out.writeInt(0);
        }
        else {
            byte[] bytes = payload.getBytes();
            int length = bytes.length;

            out.writeInt(length);
            out.write(bytes);
        }
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        int length = in.readInt();

        if (length != 0) {
            byte[] bytes = new byte[length];

            in.read(bytes);

            payload = new String(bytes);
        }
    }

    /**
     * @return String result represetnation.
     */
    public String result() {
        return payload;
    }
}
