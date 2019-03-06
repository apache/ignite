/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.marshaller.jdk;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * This class defines custom JDK object input stream.
 */
class JdkMarshallerObjectInputStream extends ObjectInputStream {
    /** */
    private final ClassLoader clsLdr;

    /** Class name filter. */
    private final IgnitePredicate<String> clsFilter;

    /**
     * @param in Parent input stream.
     * @param clsLdr Custom class loader.
     * @throws IOException If initialization failed.
     */
    JdkMarshallerObjectInputStream(InputStream in, ClassLoader clsLdr, IgnitePredicate<String> clsFilter) throws IOException {
        super(in);

        assert clsLdr != null;

        this.clsLdr = clsLdr;
        this.clsFilter = clsFilter;

        enableResolveObject(true);
    }

    /** {@inheritDoc} */
    @Override protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        // NOTE: DO NOT CHANGE TO 'clsLoader.loadClass()'
        // Must have 'Class.forName()' instead of clsLoader.loadClass()
        // due to weird ClassNotFoundExceptions for arrays of classes
        // in certain cases.
        return U.forName(desc.getName(), clsLdr, clsFilter);
    }

    /** {@inheritDoc} */
    @Override protected Object resolveObject(Object o) throws IOException {
        if (o != null && o.getClass().equals(JdkMarshallerDummySerializable.class))
            return new Object();

        return super.resolveObject(o);
    }
}
