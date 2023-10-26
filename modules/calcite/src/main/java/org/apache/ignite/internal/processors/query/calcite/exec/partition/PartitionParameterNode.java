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

package org.apache.ignite.internal.processors.query.calcite.exec.partition;

import org.apache.calcite.rex.RexDynamicParam;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;

/** */
public class PartitionParameterNode extends PartitionSingleNode {
    /** */
    private final Class<?> colType;

    /** */
    private final RexDynamicParam param;

    /** */
    public PartitionParameterNode(int cacheId, RexDynamicParam param, Class<?> colType) {
        super(cacheId);
        this.param = param;
        this.colType = colType;
    }

    /** {@inheritDoc} */
    @Override Integer applySingle(PartitionPruningContext ctx) {
        int idx = param.getIndex();

        Object val = TypeUtils.toInternal(ctx.dataContext(), ctx.resolveParameter(idx), colType);

        if (val == null)
            return null;

        AffinityService affSvc = ctx.affinityService();

        return affSvc.affinity(cacheId()).applyAsInt(val);
    }
}
