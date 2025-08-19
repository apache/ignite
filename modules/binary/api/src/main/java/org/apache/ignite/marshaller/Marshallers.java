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

package org.apache.ignite.marshaller;

import java.io.Serializable;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteCommonsSystemProperties.IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID;

/**
 * Factory to create implementation of {@link Marshaller}.
 */
public class Marshallers {
    /** Flag whether class caching should be used by the current thread. */
    public static final ThreadLocal<Boolean> USE_CACHE = ThreadLocal.withInitial(() -> Boolean.TRUE);

    /** Use default {@code serialVersionUID} for {@link Serializable} classes. */
    public static final boolean USE_DFLT_SUID =
        IgniteCommonsSystemProperties.getBoolean(IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID, false);

    /** Streams factory implementation. */
    private static final MarshallersFactory factory;

    static {
        Iterator<MarshallersFactory> factories = CommonUtils.loadService(MarshallersFactory.class).iterator();

        A.ensure(
            factories.hasNext(),
            "Implementation for MarshallersFactory service not found. Please add ignite-binary-impl to classpath"
        );

        factory = factories.next();
    }

    /** @return Default instance of {@link JdkMarshaller}. */
    public static JdkMarshaller jdk() {
        return factory.jdk();
    }

    /**
     * @param clsFilter Class filter.
     * @return Filtered instance of {@link JdkMarshaller}.
     */
    public static JdkMarshaller jdk(@Nullable IgnitePredicate<String> clsFilter) {
        return factory.jdk(clsFilter);
    }

    /** @return Optimized marshaller instance. */
    public static OptimizedMarshaller optimized() {
        return factory.optimized();
    }

    /**
     * Creates new marshaller providing whether it should
     * require {@link Serializable} interface or not.
     *
     * @param requireSer Whether to require {@link Serializable}.
     * @return Optimized marshaller instance.
     */
    public static OptimizedMarshaller optimized(boolean requireSer) {
        return factory.optimized(requireSer);
    }

    /**
     * Marshals object to byte array.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param marsh Marshaller.
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static byte[] marshal(Marshaller marsh, Object obj) throws IgniteCheckedException {
        assert marsh != null;

        try {
            return marsh.marshal(obj);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Marshals object to byte array.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param marsh Marshaller.
     * @param obj Object to marshal.
     * @param out Output stream.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static void marshal(Marshaller marsh, @Nullable Object obj, OutputStream out)
        throws IgniteCheckedException {
        assert marsh != null;

        try {
            marsh.marshal(obj, out);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }
}
