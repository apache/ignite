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

package org.apache.ignite.internal.processors.platform.utils;

import org.apache.ignite.internal.portable.*;
import org.apache.ignite.internal.processors.platform.*;
import org.apache.ignite.internal.processors.platform.callback.*;
import org.apache.ignite.internal.processors.platform.memory.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

/**
 * Interop future utils.
 */
public class PlatformFutureUtils {
    /** Future type: byte. */
    public static final int TYP_BYTE = 1;

    /** Future type: boolean. */
    public static final int TYP_BOOL = 2;

    /** Future type: short. */
    public static final int TYP_SHORT = 3;

    /** Future type: char. */
    public static final int TYP_CHAR = 4;

    /** Future type: int. */
    public static final int TYP_INT = 5;

    /** Future type: float. */
    public static final int TYP_FLOAT = 6;

    /** Future type: long. */
    public static final int TYP_LONG = 7;

    /** Future type: double. */
    public static final int TYP_DOUBLE = 8;

    /** Future type: object. */
    public static final int TYP_OBJ = 9;

    /**
     * Listen future.
     *
     * @param ctx Interop context.
     * @param fut Java future.
     * @param futPtr Native future pointer.
     * @param typ Expected return type.
     */
    public static void listen(final PlatformContext ctx, IgniteFuture fut, final long futPtr, final int typ) {
        listen(ctx, new FutureListenable(fut), futPtr, typ, null);
    }

    /**
     * Listen future.
     *
     * @param ctx Interop context.
     * @param fut Java future.
     * @param futPtr Native future pointer.
     * @param typ Expected return type.
     * @param writer Writer.
     */
    public static void listen(final PlatformContext ctx, IgniteFuture fut, final long futPtr, final int typ,
        Writer writer) {
        listen(ctx, new FutureListenable(fut), futPtr, typ, writer);
    }

    /**
     * Listen future.
     *
     * @param ctx Interop context.
     * @param fut Java future.
     * @param futPtr Native future pointer.
     * @param writer Writer.
     */
    public static void listen(final PlatformContext ctx, IgniteFuture fut, final long futPtr, Writer writer) {
        listen(ctx, new FutureListenable(fut), futPtr, TYP_OBJ, writer);
    }

    /**
     * Listen future.
     *
     * @param ctx Interop context.
     * @param listenable Listenable entry.
     * @param futPtr Native future pointer.
     * @param typ Expected return type.
     * @param writer Optional writer.
     */
    @SuppressWarnings("unchecked")
    private static void listen(final PlatformContext ctx, Listenable listenable, final long futPtr, final int typ,
        @Nullable final Writer writer) {
        final PlatformCallbackGateway gate = ctx.gateway();

        listenable.listen(new IgniteBiInClosure<Object, Throwable>() {
            private static final long serialVersionUID = 0L;

            @Override public void apply(Object res, Throwable err) {
                if (writer != null && writeToWriter(res, err, ctx, writer, futPtr))
                    return;

                if (err != null) {
                    writeFutureError(ctx, futPtr, err);

                    return;
                }

                try {
                    if (typ == TYP_OBJ) {
                        if (res == null)
                            gate.futureNullResult(futPtr);
                        else {
                            try (PlatformMemory mem = ctx.memory().allocate()) {
                                PlatformOutputStream out = mem.output();

                                PortableRawWriterEx outWriter = ctx.writer(out);

                                outWriter.writeObjectDetached(res);

                                out.synchronize();

                                gate.futureObjectResult(futPtr, mem.pointer());
                            }
                        }
                    }
                    else if (res == null)
                        gate.futureNullResult(futPtr);
                    else {
                        switch (typ) {
                            case TYP_BYTE:
                                gate.futureByteResult(futPtr, (byte) res);

                                break;

                            case TYP_BOOL:
                                gate.futureBoolResult(futPtr, (boolean) res ? 1 : 0);

                                break;

                            case TYP_SHORT:
                                gate.futureShortResult(futPtr, (short) res);

                                break;

                            case TYP_CHAR:
                                gate.futureCharResult(futPtr, (char) res);

                                break;

                            case TYP_INT:
                                gate.futureIntResult(futPtr, (int) res);

                                break;

                            case TYP_FLOAT:
                                gate.futureFloatResult(futPtr, (float) res);

                                break;

                            case TYP_LONG:
                                gate.futureLongResult(futPtr, (long) res);

                                break;

                            case TYP_DOUBLE:
                                gate.futureDoubleResult(futPtr, (double) res);

                                break;

                            default:
                                assert false : "Should not reach this: " + typ;
                        }
                    }
                }
                catch (Throwable t) {
                    writeFutureError(ctx, futPtr, t);

                    if (t instanceof Error)
                        throw t;
                }
            }
        });
    }

    /**
     * Write future error.
     *
     * @param ctx Interop context.
     * @param futPtr Future pointer.
     * @param err Error.
     */
    private static void writeFutureError(final PlatformContext ctx, long futPtr, Throwable err) {
        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx outWriter = ctx.writer(out);

            outWriter.writeString(err.getClass().getName());
            outWriter.writeString(err.getMessage());

            PlatformUtils.writeErrorData(err, outWriter);

            out.synchronize();

            ctx.gateway().futureError(futPtr, mem.pointer());
        }
    }

    /**
     * Write result to a custom writer
     *
     * @param obj Object to write.
     * @param err Error to write.
     * @param ctx Interop context.
     * @param writer Writer.
     * @param futPtr Future pointer.
     * @return Value indicating whether custom write was performed. When false, default write will be used.
     */
    private static boolean writeToWriter(Object obj, Throwable err, PlatformContext ctx, Writer writer, long futPtr) {
        boolean canWrite = writer.canWrite(obj, err);

        if (!canWrite)
            return false;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx outWriter = ctx.writer(out);

            writer.write(outWriter, obj, err);

            out.synchronize();

            ctx.gateway().futureObjectResult(futPtr, mem.pointer());
        }

        return true;
    }

    /**
     * Writer allowing special future result handling.
     */
    public static interface Writer {
        /**
         * Write object.
         *
         * @param writer Writer.
         * @param obj Object.
         * @param err Error.
         */
        public void write(PortableRawWriterEx writer, Object obj, Throwable err);

        /**
         * Determines whether this writer can write given data.
         *
         * @param obj Object.
         * @param err Error.
         * @return Value indicating whether this writer can write given data.
         */
        public boolean canWrite(Object obj, Throwable err);
    }

    /**
     * Listenable entry.
     */
    private static interface Listenable {
        /**
         * Listen.
         *
         * @param lsnr Listener.
         */
        public void listen(IgniteBiInClosure<Object, Throwable> lsnr);
    }

    /**
     * Listenable around Ignite future.
     */
    private static class FutureListenable implements Listenable {
        /** Future. */
        private final IgniteFuture fut;

        /**
         * Constructor.
         *
         * @param fut Future.
         */
        public FutureListenable(IgniteFuture fut) {
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void listen(final IgniteBiInClosure<Object, Throwable> lsnr) {
            fut.listen(new IgniteInClosure<IgniteFuture>() {
                private static final long serialVersionUID = 0L;

                @Override public void apply(IgniteFuture fut0) {
                    try {
                        lsnr.apply(fut0.get(), null);
                    }
                    catch (Throwable err) {
                        lsnr.apply(null, err);

                        if (err instanceof Error)
                            throw err;
                    }
                }
            });
        }
    }
}
