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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base class for various tests for byte array values.
 */
public abstract class GridCacheAbstractByteArrayValuesSelfTest extends GridCommonAbstractTest {
    /** Key 1. */
    protected static final Integer KEY_1 = 1;

    /** Key 2. */
    protected static final Integer KEY_2 = 2;

    /**
     * Wrap provided values into byte array.
     *
     * @param vals Values.
     * @return Byte array.
     */
    protected byte[] wrap(int... vals) {
        byte[] res = new byte[vals.length];

        for (int i = 0; i < vals.length; i++)
            res[i] = (byte)vals[i];

        return res;
    }
}