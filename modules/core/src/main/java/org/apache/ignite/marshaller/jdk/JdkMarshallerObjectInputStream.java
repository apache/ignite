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

package org.apache.ignite.marshaller.jdk;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class defines custom JDK object input stream.
 */
class JdkMarshallerObjectInputStream extends ObjectInputStream {
    /** */
    private final ClassLoader clsLdr;

    /**
     * @param in Parent input stream.
     * @param clsLdr Custom class loader.
     * @throws IOException If initialization failed.
     */
    JdkMarshallerObjectInputStream(InputStream in, ClassLoader clsLdr) throws IOException {
        super(in);

        assert clsLdr != null;

        this.clsLdr = clsLdr;

        enableResolveObject(true);
    }

    /** {@inheritDoc} */
    @Override protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        // NOTE: DO NOT CHANGE TO 'clsLoader.loadClass()'
        // Must have 'Class.forName()' instead of clsLoader.loadClass()
        // due to weird ClassNotFoundExceptions for arrays of classes
        // in certain cases.
        return U.forName(desc.getName(), clsLdr);
    }

    /** {@inheritDoc} */
    @Override protected Object resolveObject(Object o) throws IOException {
        if (o != null && o.getClass().equals(JdkMarshallerDummySerializable.class))
            return new Object();

        return super.resolveObject(o);
    }
}
