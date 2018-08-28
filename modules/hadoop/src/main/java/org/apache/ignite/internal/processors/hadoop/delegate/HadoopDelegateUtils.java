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
import org.apache.ignite.hadoop.fs.IgniteHadoopFileSystemCounterWriter;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.fs.KerberosHadoopFileSystemFactory;
import org.apache.ignite.internal.processors.hadoop.HadoopClassLoader;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for Hadoop delegates.
 */
public class HadoopDelegateUtils {
    /** Secondary file system delegate class. */
    private static final String SECONDARY_FILE_SYSTEM_CLS =
        "org.apache.ignite.internal.processors.hadoop.impl.delegate.HadoopIgfsSecondaryFileSystemDelegateImpl";

    /** Default file system factory class. */
    private static final String DFLT_FACTORY_CLS =
        "org.apache.ignite.internal.processors.hadoop.impl.delegate.HadoopDefaultFileSystemFactoryDelegate";

    /** Factory proxy to delegate class name mapping. */
    private static final Map<String, String> FACTORY_CLS_MAP;

    /** Counter writer delegate implementation. */
    private static final String COUNTER_WRITER_DELEGATE_CLS =
        "org.apache.ignite.internal.processors.hadoop.impl.delegate.HadoopFileSystemCounterWriterDelegateImpl";

    static {
        FACTORY_CLS_MAP = new HashMap<>();

        FACTORY_CLS_MAP.put(BasicHadoopFileSystemFactory.class.getName(),
            "org.apache.ignite.internal.processors.hadoop.impl.delegate.HadoopBasicFileSystemFactoryDelegate");

        FACTORY_CLS_MAP.put(CachingHadoopFileSystemFactory.class.getName(),
            "org.apache.ignite.internal.processors.hadoop.impl.delegate.HadoopCachingFileSystemFactoryDelegate");

        FACTORY_CLS_MAP.put(KerberosHadoopFileSystemFactory.class.getName(),
            "org.apache.ignite.internal.processors.hadoop.impl.delegate.HadoopKerberosFileSystemFactoryDelegate");
    }

    /**
     * Create delegate for secondary file system.
     *
     * @param ldr Hadoop class loader.
     * @param proxy Proxy.
     * @return Delegate.
     */
    public static HadoopIgfsSecondaryFileSystemDelegate secondaryFileSystemDelegate(HadoopClassLoader ldr,
        IgniteHadoopIgfsSecondaryFileSystem proxy) {
        return newInstance(SECONDARY_FILE_SYSTEM_CLS, ldr, proxy);
    }

    /**
     * Create delegate for certain file system factory.
     *
     * @param proxy Proxy.
     * @return Delegate.
     */
    public static HadoopFileSystemFactoryDelegate fileSystemFactoryDelegate(ClassLoader ldr, Object proxy) {
        String clsName = FACTORY_CLS_MAP.get(proxy.getClass().getName());

        if (clsName == null)
            clsName = DFLT_FACTORY_CLS;

        return newInstance(clsName, ldr, proxy);
    }

    /**
     * Create delegate for Hadoop counter writer.
     *
     * @param ldr Class loader.
     * @param proxy Proxy.
     * @return Delegate.
     */
    public static HadoopFileSystemCounterWriterDelegate counterWriterDelegate(ClassLoader ldr,
        IgniteHadoopFileSystemCounterWriter proxy) {
        return newInstance(COUNTER_WRITER_DELEGATE_CLS, ldr, proxy);
    }

    /**
     * Get new delegate instance.
     *
     * @param clsName Class name.
     * @param ldr Optional class loader.
     * @param proxy Proxy.
     * @return Instance.
     */
    @SuppressWarnings("unchecked")
    private static <T> T newInstance(String clsName, @Nullable ClassLoader ldr, Object proxy) {
        try {
            Class delegateCls = ldr == null ? Class.forName(clsName) : Class.forName(clsName, true, ldr);

            Constructor[] ctors = delegateCls.getConstructors();

            assert ctors.length == 1;

            Object res = ctors[0].newInstance(proxy);

            return (T)res;
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException("Failed to instantiate delegate for proxy [proxy=" + proxy +
                ", delegateClsName=" + clsName + ']', e);
        }
    }

    /**
     * Private constructor.
     */
    private HadoopDelegateUtils() {
        // No-op.
    }
}
