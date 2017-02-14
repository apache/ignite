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

package org.apache.ignite.math.impls;

import org.apache.ignite.math.*;

/**
 * TODO: add description.
 */
public abstract class AbstractVectorStorage implements VectorStorage {
    private StorageOpsKinds ops;

    /**
     * 
     */
    public AbstractVectorStorage() {
        // No-op.
    }

    /**
     *
     * @param ops Delegating instance.
     */
    public AbstractVectorStorage(StorageOpsKinds ops) {
        assert ops != null;

        this.ops = ops;
    }

    @Override
    public boolean isSequentialAccess() {
        return ops.isSequentialAccess();
    }

    @Override
    public boolean isDense() {
        return ops.isDense();
    }

    @Override
    public double getLookupCost() {
        return ops.getLookupCost();
    }

    @Override
    public boolean isAddConstantTime() {
        return ops.isAddConstantTime();
    }

    @Override
    public boolean isArrayBased() {
        return ops.isArrayBased();
    }
}
