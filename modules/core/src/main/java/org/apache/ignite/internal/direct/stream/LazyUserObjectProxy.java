/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.direct.stream;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryMarshaller;

/** */
public class LazyUserObjectProxy implements InvocationHandler {
    /** */
    private final byte[] bytes;

    /** */
    private final BinaryMarshaller marsh;

    /** */
    private volatile Object obj;

    /** */
    public LazyUserObjectProxy(byte[] bytes, BinaryMarshaller marsh) {
        this.bytes = bytes;
        this.marsh = marsh;
    }

    /** */
    private Object resolve() {
        if (obj != null)
            return obj;

        synchronized (this) {
            if (obj == null) {
                ClassLoader clsLdr = Thread.currentThread().getContextClassLoader();

                try {
                    obj = marsh.unmarshal(bytes, clsLdr);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }

        return obj;
    }

    /** {@inheritDoc} */
    @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method == null)
            throw new IgniteException("LazyUserObjectProxy invocation failed: Method is null!");

        switch (method.getName()) {
            case "get":
                return resolve();

            case "isResolved":
                return obj != null;

            case "toString":
                return "LazyUserObject[resolved=" + (obj != null) + "]";

            default:
                return method.invoke(this, args);
        }
    }
}
