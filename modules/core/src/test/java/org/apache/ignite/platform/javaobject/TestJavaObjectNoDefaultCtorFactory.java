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
import org.apache.ignite.platform.PlatformJavaObjectFactory;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Test factory.
 */
@SuppressWarnings("unused")
public class TestJavaObjectNoDefaultCtorFactory implements PlatformJavaObjectFactory {
    /** Injected node. */
    @IgniteInstanceResource
    public Ignite node;

    /** */
    private boolean fBoolean;

    /** */
    private byte fByte;

    /** */
    private short fShort;

    /** */
    private char fChar;

    /** */
    private int fInt;

    /** */
    private long fLong;

    /** */
    private float fFloat;

    /** */
    private double fDouble;

    /** */
    private Object fObj;

    /** Integer field. */
    private Integer fIntBoxed;

    /** {@inheritDoc} */
    @Override public Object create() {
        return new TestJavaObjectNoDefaultCtor(fBoolean, fByte, fShort, fChar, fInt, fLong, fFloat, fDouble, fObj,
            fIntBoxed, node);
    }
}
