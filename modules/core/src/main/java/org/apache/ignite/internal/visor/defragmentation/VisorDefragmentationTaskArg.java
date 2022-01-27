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

package org.apache.ignite.internal.visor.defragmentation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class VisorDefragmentationTaskArg extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private VisorDefragmentationOperation operation;

    /** */
    private List<String> cacheNames;

    /** Empty constructor for serialization. */
    public VisorDefragmentationTaskArg() {
        // No-op.
    }

    /** */
    public VisorDefragmentationTaskArg(
        VisorDefragmentationOperation operation,
        List<String> cacheNames
    ) {

        this.operation = operation;
        this.cacheNames = cacheNames;
    }

    /** */
    public VisorDefragmentationOperation operation() {
        return operation;
    }

    /** */
    public List<String> cacheNames() {
        return cacheNames;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, operation);

        U.writeCollection(out, cacheNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        operation = U.readEnum(in, VisorDefragmentationOperation.class);

        cacheNames = U.readList(in);
    }
}
