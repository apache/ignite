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
import java.util.Map;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 *
 */
public class VisorValidateIndexesJobResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Results of indexes validation from node. */
    private Map<PartitionKey, ValidateIndexesPartitionResult> res;

    /**
     * @param res Results of indexes validation from node.
     */
    public VisorValidateIndexesJobResult(Map<PartitionKey, ValidateIndexesPartitionResult> res) {
        this.res = res;
    }

    /**
     * For externalization only.
     */
    public VisorValidateIndexesJobResult() {
    }

    /**
     * @return Results of indexes validation from node.
     */
    public Map<PartitionKey, ValidateIndexesPartitionResult> response() {
        return res;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, res);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        res = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorValidateIndexesJobResult.class, this);
    }
}
