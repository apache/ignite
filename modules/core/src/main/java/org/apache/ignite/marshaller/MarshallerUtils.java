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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public final class MarshallerUtils {
    /**
     *
     */
    private MarshallerUtils() {
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object.
     * @param out output stream.
     * @param kernalCtx kernal context.
     * @throws IgniteCheckedException
     */
    public static void marshal(final Marshaller marshaller, final @Nullable Object obj,
        final OutputStream out, final GridKernalContext kernalCtx) throws IgniteCheckedException {
        marshal(marshaller, obj, out, getConfig(kernalCtx));
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object.
     * @param kernalCtx kernal context.
     * @return serialized.
     * @throws IgniteCheckedException
     */
    public static byte[] marshal(final Marshaller marshaller, @Nullable Object obj,
        final GridKernalContext kernalCtx) throws IgniteCheckedException {
        return marshal(marshaller, obj, getConfig(kernalCtx));
    }

    /**
     *
     * @param marshaller marshaller.
     * @param in input stream.
     * @param clsLdr class loader.
     * @param kernalCtx kernal context.
     * @param <T> target type.
     * @return deserialized object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshal(final Marshaller marshaller, InputStream in, @Nullable ClassLoader clsLdr,
        final GridKernalContext kernalCtx) throws IgniteCheckedException {
        return unmarshal(marshaller, in, clsLdr, getConfig(kernalCtx));
    }

    /**
     *
     * @param marshaller marshaller.
     * @param arr byte array.
     * @param clsLdr class loader.
     * @param kernalCtx kernal context.
     * @param <T> target type
     * @return deserialized object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshal(final Marshaller marshaller, byte[] arr, @Nullable ClassLoader clsLdr,
        final GridKernalContext kernalCtx) throws IgniteCheckedException {
        return unmarshal(marshaller, arr, clsLdr, getConfig(kernalCtx));
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object.
     * @param out output stream.
     * @param igniteCfg ignite config.
     * @throws IgniteCheckedException
     */
    public static void marshal(final Marshaller marshaller, final @Nullable Object obj,
        final OutputStream out, final IgniteConfiguration igniteCfg) throws IgniteCheckedException {
        final IgniteConfiguration cfg = setCfg(igniteCfg);

        try {
            marshaller.marshal(obj, out);
        } finally {
            restoreCfg(cfg);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object.
     * @param igniteCfg ignite config.
     * @return serialized.
     * @throws IgniteCheckedException
     */
    public static byte[] marshal(final Marshaller marshaller, @Nullable Object obj,
        final IgniteConfiguration igniteCfg) throws IgniteCheckedException {
        final IgniteConfiguration cfg = setCfg(igniteCfg);

        try {
            return marshaller.marshal(obj);
        } finally {
            restoreCfg(cfg);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param in input stream.
     * @param clsLdr class loader.
     * @param igniteCfg ignite config.
     * @param <T> target type.
     * @return deserialized object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshal(final Marshaller marshaller, InputStream in, @Nullable ClassLoader clsLdr,
        final IgniteConfiguration igniteCfg) throws IgniteCheckedException {
        final IgniteConfiguration cfg = setCfg(igniteCfg);

        try {
            return marshaller.unmarshal(in, clsLdr);
        } finally {
            restoreCfg(cfg);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param arr byte array.
     * @param clsLdr class loader.
     * @param igniteCfg ignite config.
     * @param <T> target type
     * @return deserialized object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshal(final Marshaller marshaller, byte[] arr, @Nullable ClassLoader clsLdr,
        final IgniteConfiguration igniteCfg) throws IgniteCheckedException {
        final IgniteConfiguration cfg = setCfg(igniteCfg);

        try {
            return marshaller.unmarshal(arr, clsLdr);
        } finally {
            restoreCfg(cfg);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object
     * @param clsLdr class loader.
     * @param igniteCfg ignite config.
     * @param <T> target type.
     * @return deserialized value.
     * @throws IgniteCheckedException
     */
    public static <T> T clone(final Marshaller marshaller, T obj, @Nullable ClassLoader clsLdr,
        final IgniteConfiguration igniteCfg) throws IgniteCheckedException {
        final IgniteConfiguration cfg = setCfg(igniteCfg);

        try {
            return marshaller.unmarshal(marshaller.marshal(obj), clsLdr);
        } finally {
            restoreCfg(cfg);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object
     * @param clsLdr class loader.
     * @param kernalCtx kernal context.
     * @param <T> target type.
     * @return deserialized value.
     * @throws IgniteCheckedException
     */
    public static <T> T clone(final Marshaller marshaller, T obj, @Nullable ClassLoader clsLdr,
        final GridKernalContext kernalCtx) throws IgniteCheckedException {
        return clone(marshaller, obj, clsLdr, getConfig(kernalCtx));
    }

    /**
     *
     * @param gridMarshaller grid marshaller.
     * @param obj object.
     * @param off offset.
     * @param igniteCfg ignite config.
     * @return serialized data.
     * @throws IOException
     */
    public static ByteBuffer marshal(GridClientMarshaller gridMarshaller, Object obj, int off,
        IgniteConfiguration igniteCfg) throws IOException {
        final IgniteConfiguration cfg = setCfg(igniteCfg);

        try {
            return gridMarshaller.marshal(obj, off);
        } finally {
            restoreCfg(cfg);
        }
    }

    /**
     *
     * @param gridMarshaller grid marshaller.
     * @param bytes byte array.
     * @param igniteCfg ignite config.
     * @param <T> target type.
     * @return deserialized value.
     * @throws IOException
     */
    public static <T> T unmarshal(GridClientMarshaller gridMarshaller, byte[] bytes,
        IgniteConfiguration igniteCfg) throws IOException {
        final IgniteConfiguration cfg = setCfg(igniteCfg);

        try {
            return gridMarshaller.unmarshal(bytes);
        } finally {
            restoreCfg(cfg);
        }
    }

    /**
     *
     * @param igniteCfg itgnite config.
     * @return old ignite config
     */
    private static IgniteConfiguration setCfg(final IgniteConfiguration igniteCfg) {
        final IgniteConfiguration cfg = IgnitionEx.getIgniteCfgThreadLocal();

        if (igniteCfg != cfg)
            IgnitionEx.setIgniteCfgThreadLocal(igniteCfg);

        return cfg;
    }

    /**
     *
     * @param igniteCfg ignite config
     */
    private static void restoreCfg(final IgniteConfiguration igniteCfg) {
        IgnitionEx.setIgniteCfgThreadLocal(igniteCfg);
    }

    /**
     *
     * @param kernalCtx kernal context.
     * @return ignite config or null.
     */
    private static IgniteConfiguration getConfig(final @Nullable GridKernalContext kernalCtx) {
        return kernalCtx == null ? null : kernalCtx.config();
    }
}
