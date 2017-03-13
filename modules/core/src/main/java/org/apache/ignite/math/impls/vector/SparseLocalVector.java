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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.*;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.vector.*;
import java.util.*;

/**
 * Local on-heap sparse vector based on hash map storage.
 */
public class SparseLocalVector extends AbstractVector implements StorageConstants {
    /**
     *
     */
    public SparseLocalVector() {
        // No-op.
    }

    /**
     *
     * @param size
     * @param acsMode
     */
    public SparseLocalVector(int size, int acsMode) {
        assertAccessMode(acsMode);

        setStorage(new SparseLocalOnHeapVectorStorage(size, acsMode));
    }

    /**
     * @param args
     */
    public SparseLocalVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size") && args.containsKey("acsMode")) {
            int size = (int)args.get("size");
            int acsMode = (int)args.get("acsMode");

            assertAccessMode(acsMode);

            setStorage(new SparseLocalOnHeapVectorStorage(size, acsMode));
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    private SparseLocalOnHeapVectorStorage storage() {
        return (SparseLocalOnHeapVectorStorage)getStorage();
    }

    @Override public Vector like(int crd) {
        SparseLocalOnHeapVectorStorage sto = storage();

        return new SparseLocalVector(crd, sto.getAccessMode());
    }

    @Override public Matrix likeMatrix(int rows, int cols) {
        return null; // TODO
    }

    /** {@inheritDoc} */
    @Override public Vector times(double x) {
        if (x == 0.0)
            return assign(0);
        else
            return super.times(x);
    }
}
