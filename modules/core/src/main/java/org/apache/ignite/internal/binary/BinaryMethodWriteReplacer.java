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

package org.apache.ignite.internal.binary;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.ignite.binary.BinaryObjectException;
import org.jetbrains.annotations.Nullable;

/**
 * Write replacer based on method invocation.
 */
public class BinaryMethodWriteReplacer implements BinaryWriteReplacer {
    /** Method. */
    private final Method mthd;

    /**
     * Constructor.
     *
     * @param mthd Method.
     */
    public BinaryMethodWriteReplacer(Method mthd) {
        assert mthd != null;

        this.mthd = mthd;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object replace(Object target) {
        try {
            return mthd.invoke(target);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof BinaryObjectException)
                throw (BinaryObjectException)e.getTargetException();

            throw new BinaryObjectException("Failed to execute writeReplace() method on " + target, e);
        }
    }
}
