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

package org.apache.ignite.platform.javaobject;

import org.apache.ignite.Ignite;

/**
 * Test Java object without default constructor.
 */
public class TestJavaObjectNoDefaultCtor extends TestJavaObject {
    /** Node. */
    public Ignite node;

    /**
     * Constructor.
     *
     * @param fBoolean Boolean field.
     * @param fByte Byte field.
     * @param fShort Short field.
     * @param fChar Char field.
     * @param fInt Integer field.
     * @param fLong Long field.
     * @param fDouble Double field.
     * @param fFloat Float field.
     * @param fObj Object field.
     * @param fIntBoxed Integer boxed field.
     */
    public TestJavaObjectNoDefaultCtor(boolean fBoolean, byte fByte, short fShort, char fChar, int fInt, long fLong,
        float fFloat, double fDouble, Object fObj, Integer fIntBoxed, Ignite node) {
        super(fBoolean, fByte, fShort, fChar, fInt, fLong, fFloat, fDouble, fObj, fIntBoxed);

        this.node = node;
    }
}
