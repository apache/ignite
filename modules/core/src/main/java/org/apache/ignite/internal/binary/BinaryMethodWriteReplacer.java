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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryObjectException;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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
