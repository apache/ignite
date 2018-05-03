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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 *
 */
public class VisorContentionTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Min queue size. */
    private int minQueueSize;

    /** Max print size. */
    private int maxPrint;

    /**
     * @param minQueueSize Min queue size.
     * @param maxPrint Max print.
     */
    public VisorContentionTaskArg(int minQueueSize, int maxPrint) {
        this.minQueueSize = minQueueSize;
        this.maxPrint = maxPrint;
    }

    /**
     * For externalization only.
     */
    public VisorContentionTaskArg() {
    }

    /**
     * @return Min queue size.
     */
    public int minQueueSize() {
        return minQueueSize;
    }

    /**
     * @return Max print size.
     */
    public int maxPrint() {
        return maxPrint;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(minQueueSize);
        out.writeInt(maxPrint);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        minQueueSize = in.readInt();
        maxPrint = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorContentionTaskArg.class, this);
    }
}
