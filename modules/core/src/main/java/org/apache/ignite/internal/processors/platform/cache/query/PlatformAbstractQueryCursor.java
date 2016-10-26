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

package org.apache.ignite.internal.processors.platform.cache.query;

import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;

/**
 *
 */
public abstract class PlatformAbstractQueryCursor<T> extends PlatformAbstractTarget implements AutoCloseable {
    /** Get multiple entries. */
    private static final int OP_GET_ALL = 1;

    /** Get all entries. */
    private static final int OP_GET_BATCH = 2;

    /** Get single entry. */
    private static final int OP_GET_SINGLE = 3;

    /** Start iterating. */
    private static final int OP_ITERATOR = 4;

    /** Close iterator. */
    private static final int OP_ITERATOR_CLOSE = 5;

    /** Close iterator. */
    private static final int OP_ITERATOR_HAS_NEXT = 6;

    /** Underlying cursor. */
    private final QueryCursorEx<T> cursor;

    /** Batch size size. */
    private final int batchSize;

    /** Underlying iterator. */
    private Iterator<T> iter;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param cursor Underlying cursor.
     * @param batchSize Batch size.
     */
    public PlatformAbstractQueryCursor(PlatformContext platformCtx, QueryCursorEx<T> cursor, int batchSize) {
        super(platformCtx);

        this.cursor = cursor;
        this.batchSize = batchSize;
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, final BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_GET_BATCH: {
                assert iter != null : "iterator() has not been called";

                try {
                    int cntPos = writer.reserveInt();

                    int cnt = 0;

                    while (cnt < batchSize && iter.hasNext()) {
                        write(writer, iter.next());

                        cnt++;
                    }

                    writer.writeInt(cntPos, cnt);
                }
                catch (Exception err) {
                    throw PlatformUtils.unwrapQueryException(err);
                }

                break;
            }

            case OP_GET_SINGLE: {
                assert iter != null : "iterator() has not been called";

                try {
                    if (iter.hasNext()) {
                        write(writer, iter.next());

                        return;
                    }
                }
                catch (Exception err) {
                    throw PlatformUtils.unwrapQueryException(err);
                }

                throw new IgniteCheckedException("No more data available.");
            }

            case OP_GET_ALL: {
                try {
                    int pos = writer.reserveInt();

                    Consumer<T> consumer = new Consumer<>(this, writer);

                    cursor.getAll(consumer);

                    writer.writeInt(pos, consumer.cnt);
                }
                catch (Exception err) {
                    throw PlatformUtils.unwrapQueryException(err);
                }

                break;
            }

            default:
                super.processOutStream(type, writer);
        }
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_ITERATOR:
                iter = cursor.iterator();

                return TRUE;

            case OP_ITERATOR_CLOSE:
                cursor.close();

                return TRUE;

            case OP_ITERATOR_HAS_NEXT:
                assert iter != null : "iterator() has not been called";

                return iter.hasNext() ? TRUE : FALSE;
        }

        return super.processInLongOutLong(type, val);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        cursor.close();
    }

    /**
     * Write value to the stream. Extension point to perform conversions on the object before writing it.
     *
     * @param writer Writer.
     * @param val Value.
     */
    protected abstract void write(BinaryRawWriterEx writer, T val);

    /**
     * Query cursor consumer.
     */
    private static class Consumer<T> implements QueryCursorEx.Consumer<T> {
        /** Current query cursor. */
        private final PlatformAbstractQueryCursor<T> cursor;

        /** Writer. */
        private final BinaryRawWriterEx writer;

        /** Count. */
        private int cnt;

        /**
         * Constructor.
         *
         * @param writer Writer.
         */
        public Consumer(PlatformAbstractQueryCursor<T> cursor, BinaryRawWriterEx writer) {
            this.cursor = cursor;
            this.writer = writer;
        }

        /** {@inheritDoc} */
        @Override public void consume(T val) throws IgniteCheckedException {
            cursor.write(writer, val);

            cnt++;
        }
    }
}
