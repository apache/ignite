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

package org.apache.ignite.internal.processors.hadoop.delegate;

import org.apache.ignite.IgniteException;
import org.apache.ignite.hadoop.fs.BasicHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.fs.KerberosHadoopFileSystemFactory;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for Hadoop delegates.
 */
public class HadoopDelegateUtils {
    /** Proxy to delegate class name mapping. */
    private static final Map<String, String> CLS_MAP;

    static {
        CLS_MAP = new HashMap<>();

        CLS_MAP.put(proxyClassName(IgniteHadoopIgfsSecondaryFileSystem.class),
            "org.apache.ignite.internal.processors.hadoop.delegate.HadoopIgfsSecondaryFileSystemDelegateImpl");

        CLS_MAP.put(proxyClassName(BasicHadoopFileSystemFactory.class),
            "org.apache.ignite.internal.processors.hadoop.delegate.HadoopBasicFileSystemFactoryDelegate");

        CLS_MAP.put(proxyClassName(CachingHadoopFileSystemFactory.class),
            "org.apache.ignite.internal.processors.hadoop.delegate.HadoopCachingFileSystemFactoryDelegate");

        CLS_MAP.put(proxyClassName(KerberosHadoopFileSystemFactory.class),
            "org.apache.ignite.internal.processors.hadoop.delegate.HadoopKerberosFileSystemFactoryDelegate");
    }

    /**
     * Create delegate for certain proxy.
     *
     * @param proxy Proxy.
     * @return Delegate.
     */
    @SuppressWarnings("unchecked")
    public static <T> T delegate(Object proxy) {
        String delegateClsName = delegateClassName(proxy.getClass());

        try {
            Class delegateCls = Class.forName(delegateClsName);

            Constructor[] ctors = delegateCls.getConstructors();

            assert ctors.length == 1;

            Object res = ctors[0].newInstance(proxy);

            return (T)res;
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException("Failed to instantiate delegate for proxy [proxy=" + proxy +
                ", delegateClsName=" + delegateClsName + ']', e);
        }
    }

    /**
     * Get delegate class name.
     *
     * @param proxyCls Proxy class.
     * @return Delegate class name.
     */
    private static String delegateClassName(Class proxyCls) {
        assert proxyCls != null;

        String proxyClsName = proxyClassName(proxyCls);

        String delegateClsName = CLS_MAP.get(proxyClsName);

        if (delegateClsName == null)
            throw new IgniteException("No delegate for proxy class: " + proxyClsName);

        return delegateClsName;
    }

    /**
     * Get proxy class name.
     *
     * @param proxyCls Proxy class.
     * @return Class name.
     */
    private static String proxyClassName(Class proxyCls) {
        assert proxyCls != null;

        return proxyCls.getClass().getName();
    }

    /**
     * Private constructor.
     */
    private HadoopDelegateUtils() {
        // No-op.
    }
}
