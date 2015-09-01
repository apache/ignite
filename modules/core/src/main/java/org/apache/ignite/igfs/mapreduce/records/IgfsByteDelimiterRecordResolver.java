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

package org.apache.ignite.igfs.mapreduce.records;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.mapreduce.IgfsFileRange;
import org.apache.ignite.igfs.mapreduce.IgfsRecordResolver;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Record resolver which adjusts records based on provided delimiters. Both start position and length are
 * shifted to the right, based on delimiter positions.
 * <p>
 * Note that you can use {@link IgfsStringDelimiterRecordResolver} if your delimiter is a plain string.
 */
public class IgfsByteDelimiterRecordResolver implements IgfsRecordResolver, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Delimiters. */
    private byte[][] delims;

    /** Maximum delimiter length. */
    @GridToStringExclude
    private int maxDelimLen;

    /**
     * Empty constructor required for {@link Externalizable} support.
     */
    public IgfsByteDelimiterRecordResolver() {
        // No-op.
    }

    /**
     * Creates delimiter-based record resolver.
     *
     * @param delims Delimiters.
     */
    public IgfsByteDelimiterRecordResolver(byte[]... delims) {
        if (delims == null || delims.length == 0)
            throw new IllegalArgumentException("Delimiters cannot be null or empty.");

        this.delims = delims;

        int maxDelimLen = 0;

        for (byte[] delim : delims) {
            if (delim == null)
                throw new IllegalArgumentException("Delimiter cannot be null.");
            else if (maxDelimLen < delim.length)
                maxDelimLen = delim.length;
        }

        this.maxDelimLen = maxDelimLen;
    }

    /** {@inheritDoc} */
    @Override public IgfsFileRange resolveRecords(IgniteFileSystem fs, IgfsInputStream stream,
        IgfsFileRange suggestedRecord) throws IgniteException, IOException {
        long suggestedStart = suggestedRecord.start();
        long suggestedEnd = suggestedStart + suggestedRecord.length();

        IgniteBiTuple<State, Delimiter> firstDelim = findFirstDelimiter(stream, suggestedStart);

        State state = firstDelim != null ? firstDelim.getKey() : new State();

        Delimiter curDelim = firstDelim.getValue();

        while (curDelim != null && curDelim.end < suggestedStart)
            curDelim = nextDelimiter(stream, state);

        if (curDelim != null && (curDelim.end >= suggestedStart && curDelim.end < suggestedEnd) ||
            suggestedStart == 0 ) {
            // We found start delimiter.
            long start = suggestedStart == 0 ? 0 : curDelim.end;

            if (curDelim == null || curDelim.end < suggestedEnd) {
                IgniteBiTuple<State, Delimiter> lastDelim = findFirstDelimiter(stream, suggestedEnd);

                state = lastDelim != null ? firstDelim.getKey() : new State();

                curDelim = lastDelim.getValue();

                while (curDelim != null && curDelim.end < suggestedEnd)
                    curDelim = nextDelimiter(stream, state);
            }

            long end = curDelim != null ? curDelim.end : stream.position();

            return new IgfsFileRange(suggestedRecord.path(), start, end - start);
        }
        else
            // We failed to find any delimiters up to the EOS.
            return null;
    }

    /**
     * Calculate maximum delimiters length.
     *
     * @param delims Delimiters.
     * @return Maximum delimiter length.
     */
    private int maxDelimiterLength(byte[][] delims) {
        int maxDelimLen = 0;

        for (byte[] delim : delims) {
            if (delim == null)
                throw new IllegalArgumentException("Delimiter cannot be null.");
            else if (maxDelimLen < delim.length)
                maxDelimLen = delim.length;
        }

        return maxDelimLen;
    }

    /**
     * Find first delimiter. In order to achieve this we have to rewind the stream until we find the delimiter
     * which stands at least [maxDelimLen] from the start search position or until we faced stream start.
     * Otherwise we cannot be sure that delimiter position is determined correctly.
     *
     * @param stream IGFS input stream.
     * @param startPos Start search position.
     * @return The first found delimiter.
     * @throws IOException In case of IO exception.
     */
    @Nullable private IgniteBiTuple<State, Delimiter> findFirstDelimiter(IgfsInputStream stream, long startPos)
        throws IOException {
        State state;
        Delimiter delim;

        long curPos = Math.max(0, startPos - maxDelimLen);

        while (true) {
            stream.seek(curPos);

            state = new State();

            delim = nextDelimiter(stream, state);

            if (curPos == 0 || delim == null || delim.start - curPos > maxDelimLen - 1)
                break;
            else
                curPos = Math.max(0, curPos - maxDelimLen);
        }

        return F.t(state, delim);
    }

    /**
     * Resolve next delimiter.
     *
     * @param is IGFS input stream.
     * @param state Current state.
     * @return Next delimiter and updated map.
     * @throws IOException In case of exception.
     */
    private Delimiter nextDelimiter(IgfsInputStream is, State state) throws IOException {
        assert is != null;
        assert state != null;

        Map<Integer, Integer> parts = state.parts;
        LinkedList<Delimiter> delimQueue = state.delims;

        int nextByte = is.read();

        while (nextByte != -1) {
            // Process read byte.
            for (int idx = 0; idx < delims.length; idx++) {
                byte[] delim = delims[idx];

                int val = parts.containsKey(idx) ? parts.get(idx) : 0;

                if (delim[val] == nextByte) {
                    if (val == delim.length - 1) {
                        // Full delimiter is found.
                        parts.remove(idx);

                        Delimiter newDelim = new Delimiter(is.position() - delim.length, is.position());

                        // Read queue from the end looking for the "inner" delimiters.
                        boolean ignore = false;

                        int replaceIdx = -1;

                        for (int i = delimQueue.size() - 1; i >= 0; i--) {
                            Delimiter prevDelim = delimQueue.get(i);

                            if (prevDelim.start < newDelim.start) {
                                if (prevDelim.end > newDelim.start) {
                                    // Ignore this delimiter.
                                    ignore = true;

                                    break;
                                }
                            }
                            else if (prevDelim.start == newDelim.start) {
                                // Ok, we found matching delimiter.
                                replaceIdx = i;

                                break;
                            }
                        }

                        if (!ignore) {
                            if (replaceIdx >= 0)
                                delimQueue.removeAll(delimQueue.subList(replaceIdx, delimQueue.size()));

                            delimQueue.add(newDelim);
                        }
                    }
                    else
                        parts.put(idx, ++val);
                }
                else if (val != 0) {
                    if (delim[0] == nextByte) {
                        boolean shift = true;

                        for (int k = 1; k < val; k++) {
                            if (delim[k] != nextByte) {
                                shift = false;

                                break;
                            }
                        }

                        if (!shift)
                            parts.put(idx, 1);
                    }
                    else
                        // Delimiter sequence is totally broken.
                        parts.remove(idx);
                }
            }

            // Check whether we can be sure that the first delimiter will not change.
            if (!delimQueue.isEmpty()) {
                Delimiter delim = delimQueue.get(0);

                if (is.position() - delim.end >= maxDelimLen)
                    return delimQueue.poll();
            }

            nextByte = is.read();
        }

        return delimQueue.poll();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsByteDelimiterRecordResolver.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        if (delims != null) {
            out.writeBoolean(true);

            out.writeInt(delims.length);

            for (byte[] delim : delims)
                U.writeByteArray(out, delim);
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readBoolean()) {
            int len = in.readInt();

            delims = new byte[len][];

            for (int i = 0; i < len; i++)
                delims[i] = U.readByteArray(in);

            maxDelimLen = maxDelimiterLength(delims);
        }
    }

    /**
     * Delimiter descriptor.
     */
    private static class Delimiter {
        /** Delimiter start position. */
        private final long start;

        /** Delimiter end position. */
        private final long end;

        /**
         * Constructor.
         *
         * @param start Delimiter start position.
         * @param end Delimiter end position.
         */
        private Delimiter(long start, long end) {
            assert start >= 0 && end >= 0 && start <= end;

            this.start = start;
            this.end = end;
        }
    }

    /**
     * Current resolution state.
     */
    private static class State {
        /** Partially resolved delimiters. */
        private final Map<Integer, Integer> parts;

        /** Resolved delimiters which could potentially be merged. */
        private final LinkedList<Delimiter> delims;

        /**
         * Constructor.
         */
        private State() {
            parts = new HashMap<>();

            delims = new LinkedList<>();
        }
    }
}