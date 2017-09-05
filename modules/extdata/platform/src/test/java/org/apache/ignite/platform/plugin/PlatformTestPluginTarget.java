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

package org.apache.ignite.platform.plugin;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAsyncResult;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.PluginConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Test target.
 */
@SuppressWarnings("ConstantConditions")
class PlatformTestPluginTarget implements PlatformTarget {
    /** */
    private final String name;

    /** */
    private final PlatformContext platformCtx;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     */
    PlatformTestPluginTarget(PlatformContext platformCtx, String name) {
        this.platformCtx = platformCtx;

        if (name == null) {
            // Initialize from configuration.
            PlatformTestPluginConfiguration cfg = configuration(platformCtx.kernalContext().config());

            assert cfg != null;

            name = cfg.pluginProperty();
        }

        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        if (type == -1)
            throw new PlatformTestPluginException("Baz");

        return val + 1;
    }

    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        return reader.readString().length();
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader, PlatformMemory mem)
            throws IgniteCheckedException {
        return processInStreamOutLong(type, reader);
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
            throws IgniteCheckedException {
        String s = reader.readString();

        writer.writeString(s.toUpperCase());
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInStreamOutObject(int type, BinaryRawReaderEx reader)
            throws IgniteCheckedException {
        return new PlatformTestPluginTarget(platformCtx, reader.readString());
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInObjectStreamOutObjectStream(
            int type, @Nullable PlatformTarget arg, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
            throws IgniteCheckedException {
        PlatformTestPluginTarget t = (PlatformTestPluginTarget)arg;

        writer.writeString(invokeCallback(t.name));

        return new PlatformTestPluginTarget(platformCtx, t.name + reader.readString());
    }

    /**
     * Invokes the platform callback.
     *
     * @param val Value to send.
     * @return Result.
     */
    private String invokeCallback(String val) {
        PlatformMemory outMem = platformCtx.memory().allocate();
        PlatformMemory inMem = platformCtx.memory().allocate();

        PlatformOutputStream outStream = outMem.output();
        BinaryRawWriterEx writer = platformCtx.writer(outStream);

        writer.writeString(val);

        outStream.synchronize();

        platformCtx.gateway().pluginCallback(1, outMem, inMem);

        BinaryRawReaderEx reader = platformCtx.reader(inMem);

        return reader.readString();
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        writer.writeString(name);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        return new PlatformTestPluginTarget(platformCtx, name);
    }

    /** {@inheritDoc} */
    @Override public PlatformAsyncResult processInStreamAsync(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case 1: {
                // Async upper case.
                final String val = reader.readString();

                final GridFutureAdapter<String> fa = new GridFutureAdapter<String>() {
                    @Override public boolean cancel() throws IgniteCheckedException {
                        return onCancelled();
                    }
                };

                new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            Thread.sleep(500L);
                            fa.onDone(val.toUpperCase());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();

                return new PlatformAsyncResult() {
                    @Override public IgniteFuture future() {
                        //noinspection unchecked
                        return new IgniteFutureImpl(fa);
                    }

                    @Override public void write(BinaryRawWriterEx writer, Object result) {
                        writer.writeString((String) result);
                    }
                };
            }
            case 2: {
                // Exception.
                throw new PlatformTestPluginException("123");
            }
            case 3: {
                // Async exception.
                final GridFutureAdapter<String> fa = new GridFutureAdapter<>();

                new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            Thread.sleep(500L);
                            fa.onDone(new PlatformTestPluginException("x"));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();

                return new PlatformAsyncResult() {
                    @Override public IgniteFuture future() {
                        //noinspection unchecked
                        return new IgniteFutureImpl(fa);
                    }

                    @Override public void write(BinaryRawWriterEx writer, Object result) {
                        // No-op.
                    }
                };
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public Exception convertException(Exception e) {
        return e;
    }

    /**
     * Gets the plugin config.
     *
     * @param igniteCfg Ignite config.
     *
     * @return Plugin config.
     */
    private PlatformTestPluginConfiguration configuration(IgniteConfiguration igniteCfg) {
        if (igniteCfg.getPluginConfigurations() != null) {
            for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
                if (pluginCfg instanceof PlatformTestPluginConfiguration) {
                    return (PlatformTestPluginConfiguration) pluginCfg;
                }
            }
        }

        return null;
    }
}
