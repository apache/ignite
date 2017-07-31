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

package org.apache.ignite.internal.visor.file;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result for file block operation.
 */
public class VisorFileBlockTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Exception on reading of block. */
    private IOException ex;

    /** Read file block. */
    private VisorFileBlock block;

    /**
     * Default constructor.
     */
    public VisorFileBlockTaskResult() {
        // No-op.
    }

    /**
     * Create log search result with given parameters.
     *
     * @param ex Exception on reading of block.
     * @param block Read file block.
     */
    public VisorFileBlockTaskResult(IOException ex, VisorFileBlock block) {
        this.ex = ex;
        this.block = block;
    }

    /**
     * @return Exception on reading of block.
     */
    public IOException getException() {
        return ex;
    }

    /**
     * @return Read file block.
     */
    public VisorFileBlock getBlock() {
        return block;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(ex);
        out.writeObject(block);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        ex = (IOException)in.readObject();
        block = (VisorFileBlock)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorFileBlockTaskResult.class, this);
    }
}
