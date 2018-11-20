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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

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
     * @param ctx Context.
     * @param fut Java future.
     * @param futPtr Native future pointer.
     * @param typ Expected return type.
     * @return Resulting listenable.
     */
    public static PlatformListenable listen(final PlatformContext ctx, IgniteInternalFuture fut, final long futPtr,
        final int typ, PlatformTarget target) {
        PlatformListenable listenable = getListenable(fut);

        listen(ctx, listenable, futPtr, typ, null, target);

        return listenable;
    }
    /**
     * Listen future.
     *
     * @param ctx Context.
     * @param fut Java future.
     * @param futPtr Native future pointer.
     * @param typ Expected return type.
     * @return Resulting listenable.
     */
    public static PlatformListenable listen(final PlatformContext ctx, IgniteFuture fut, final long futPtr,
        final int typ, PlatformTarget target) {
        PlatformListenable listenable = getListenable(fut);

        listen(ctx, listenable, futPtr, typ, null, target);

        return listenable;
    }

    /**
     * Listen future.
     *
     * @param ctx Context.
     * @param fut Java future.
     * @param futPtr Native future pointer.
     * @param typ Expected return type.
     * @param writer Writer.
     * @return Resulting listenable.
     */
    public static PlatformListenable listen(final PlatformContext ctx, IgniteInternalFuture fut, final long futPtr,
        final int typ, Writer writer, PlatformTarget target) {
        PlatformListenable listenable = getListenable(fut);

        listen(ctx, listenable, futPtr, typ, writer, target);

        return listenable;
    }

    /**
     * Listen future.
     *
     * @param ctx Context.
     * @param fut Java future.
     * @param futPtr Native future pointer.
     * @param typ Expected return type.
     * @param writer Writer.
     * @return Resulting listenable.
     */
    public static PlatformListenable listen(final PlatformContext ctx, IgniteFuture fut, final long futPtr,
        final int typ, Writer writer, PlatformTarget target) {
        PlatformListenable listenable = getListenable(fut);

        listen(ctx, listenable, futPtr, typ, writer, target);

        return listenable;
    }

    /**
     * Listen future.
     *
     * @param ctx Context.
     * @param fut Java future.
     * @param futPtr Native future pointer.
     * @param writer Writer.
     * @return Resulting listenable.
     */
    public static PlatformListenable listen(final PlatformContext ctx, IgniteInternalFuture fut, final long futPtr,
        Writer writer, PlatformTarget target) {
        PlatformListenable listenable = getListenable(fut);

        listen(ctx, listenable, futPtr, TYP_OBJ, writer, target);

        return listenable;
    }

    /**
     * Gets the listenable.
     *
     * @param fut Future.
     * @return Platform listenable.
     */
    public static PlatformListenable getListenable(IgniteInternalFuture fut) {
        return new InternalFutureListenable(fut);
    }

    /**
     * Gets the listenable.
     *
     * @param fut Future.
     * @return Platform listenable.
     */
    public static PlatformListenable getListenable(IgniteFuture fut) {
        return new FutureListenable(fut);
    }

    /**
     * Listen future.
     *
     * @param ctx Context.
     * @param listenable Listenable entry.
     * @param futPtr Native future pointer.
     * @param typ Expected return type.
     * @param writer Optional writer.
     */
    @SuppressWarnings("unchecked")
    public static void listen(final PlatformContext ctx, PlatformListenable listenable, final long futPtr, final
        int typ, @Nullable final Writer writer, final PlatformTarget target) {
        final PlatformCallbackGateway gate = ctx.gateway();

        listenable.listen(new IgniteBiInClosure<Object, Throwable>() {
            private static final long serialVersionUID = 0L;

            @Override public void apply(Object res, Throwable err) {
                if (err instanceof Exception)
                    err = target.convertException((Exception)err);

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

                                BinaryRawWriterEx outWriter = ctx.writer(out);

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
                                gate.futureFloatResult(futPtr, Float.floatToIntBits((float) res));

                                break;

                            case TYP_LONG:
                                gate.futureLongResult(futPtr, (long) res);

                                break;

                            case TYP_DOUBLE:
                                gate.futureDoubleResult(futPtr, Double.doubleToLongBits((double)res));

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
     * @param ctx Context.
     * @param futPtr Future pointer.
     * @param err Error.
     */
    private static void writeFutureError(final PlatformContext ctx, long futPtr, Throwable err) {
        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx outWriter = ctx.writer(out);

            PlatformUtils.writeError(err, outWriter);
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
     * @param ctx Context.
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

            BinaryRawWriterEx outWriter = ctx.writer(out);

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
        public void write(BinaryRawWriterEx writer, Object obj, Throwable err);

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
     * Listenable around Ignite future.
     */
    private static class FutureListenable implements PlatformListenable {
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

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return fut.cancel();
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return fut.isCancelled();
        }
    }

    /**
     * Listenable around Ignite future.
     */
    private static class InternalFutureListenable implements PlatformListenable {
        /** Future. */
        private final IgniteInternalFuture fut;

        /**
         * Constructor.
         *
         * @param fut Future.
         */
        public InternalFutureListenable(IgniteInternalFuture fut) {
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void listen(final IgniteBiInClosure<Object, Throwable> lsnr) {
            fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                private static final long serialVersionUID = 0L;

                @Override public void apply(IgniteInternalFuture fut0) {
                    try {
                        lsnr.apply(fut0.get(), null);
                    }
                    catch (Throwable err) {
                        lsnr.apply(null, err);
                    }
                }
            });
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            return fut.cancel();
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return fut.isCancelled();
        }
    }
}