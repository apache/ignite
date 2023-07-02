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

package org.apache.ignite.internal.visor.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.management.cache.CacheScanCommandArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Task that scan cache entries.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheScanTask extends VisorOneNodeTask<CacheScanCommandArg, VisorCacheScanTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheScanJob job(CacheScanCommandArg arg) {
        return new VisorCacheScanJob(arg, debug);
    }

    /**
     * Job that stop specified caches.
     */
    private static class VisorCacheScanJob extends VisorJob<CacheScanCommandArg, VisorCacheScanTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private VisorCacheScanJob(CacheScanCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheScanTaskResult run(CacheScanCommandArg arg) {
            if (F.isEmpty(arg.cacheName()))
                throw new IllegalStateException("Cache name was not specified.");

            if (arg.limit() <= 0)
                throw new IllegalStateException("Invalid limit value.");

            List<String> titles = Arrays.asList("Key Class", "Key", "Value Class", "Value");

            int cnt = 0;
            List<List<?>> entries = new ArrayList<>();

            for (Cache.Entry<?, ?> entry : ignite.cache(arg.cacheName()).withKeepBinary()) {
                Object k = entry.getKey();
                Object v = entry.getValue();
                entries.add(Arrays.asList(typeOf(k), valueOf(k), typeOf(v), valueOf(v)));

                if (++cnt >= arg.limit())
                    break;
            }

            return new VisorCacheScanTaskResult(titles, entries);
        }

        /**
         * @param o Source object.
         * @return String representation of object class.
         */
        private static String typeOf(Object o) {
            if (o != null) {
                Class<?> clazz = o.getClass();

                return clazz.isArray() ? IgniteUtils.compact(clazz.getComponentType().getName()) + "[]"
                    : IgniteUtils.compact(o.getClass().getName());
            }
            else
                return "n/a";
        }

        /**
         * @param o Object.
         * @return String representation of value.
         */
        private static String valueOf(Object o) {
            if (o == null)
                return "null";

            if (o instanceof byte[])
                return "size=" + ((byte[])o).length;

            if (o instanceof Byte[])
                return "size=" + ((Byte[])o).length;

            if (o instanceof Object[]) {
                return "size=" + ((Object[])o).length +
                    ", values=[" + S.joinToString(Arrays.asList((Object[])o), ", ", "...", 120, 0) + "]";
            }

            if (o instanceof BinaryObject)
                return binaryToString((BinaryObject)o);

            return o.toString();
        }

        /**
         * Convert Binary object to string.
         *
         * @param obj Binary object.
         * @return String representation of Binary object.
         */
        public static String binaryToString(BinaryObject obj) {
            int hash = obj.hashCode();

            if (obj instanceof BinaryObjectEx) {
                BinaryObjectEx objEx = (BinaryObjectEx)obj;

                BinaryType meta;

                try {
                    meta = ((BinaryObjectEx)obj).rawType();
                }
                catch (BinaryObjectException ignore) {
                    meta = null;
                }

                if (meta != null) {
                    if (meta.isEnum()) {
                        try {
                            return obj.deserialize().toString();
                        }
                        catch (BinaryObjectException ignore) {
                            // NO-op.
                        }
                    }

                    SB buf = new SB(meta.typeName());

                    if (meta.fieldNames() != null) {
                        buf.a(" [hash=").a(hash);

                        for (String name : meta.fieldNames()) {
                            Object val = objEx.field(name);

                            buf.a(", ").a(name).a('=').a(val);
                        }

                        buf.a(']');

                        return buf.toString();
                    }
                }
            }

            return S.toString(obj.getClass().getSimpleName(),
                "hash", hash, false,
                "typeId", obj.type().typeId(), true);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheScanJob.class, this);
        }
    }
}
