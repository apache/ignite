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

package org.apache.ignite.internal.util.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Print stream that prints each thread group into a separate buffer. Use
 * any of the {@code purge(...)} methods to print the contents of the buffer
 * to the parent base stream or to a stream of your choice.
 */
public class GridTestPrintStream extends PrintStream {
    /** */
    private final Map<ThreadGroup, BytePrintStream> streams = new HashMap<>();

    /** */
    private final ThreadGroup baseGrp;

    /**
     * Creates test print stream around the base stream passed in.
     * Root thread group is the parent of current thread group.
     *
     * @param out Base print stream.
     */
    public GridTestPrintStream(PrintStream out) {
        super(out);

        baseGrp = Thread.currentThread().getThreadGroup().getParent();
    }

    /**
     * Creates test print stream around the base stream passed in also
     * specifying root thread group.
     *
     * @param out Base print stream.
     * @param baseGrp Thread group.
     */
    public GridTestPrintStream(PrintStream out, ThreadGroup baseGrp) {
        super(out);

        assert baseGrp != null;

        this.baseGrp = baseGrp;
    }

    /**
     * Gets print stream for current thread group.
     *
     * @param release Whether or not to clear all the memory associated with thread group.
     * @return Gets {@link PrintStream} for current thread group.
     */
    private BytePrintStream out(boolean release) {
        BytePrintStream out;

        synchronized (streams) {
            ThreadGroup grp = Thread.currentThread().getThreadGroup();

            while (grp != null && grp.getParent() != baseGrp)
                grp = grp.getParent();

            out = release == true ? streams.remove(grp) : streams.get(grp);

            if (out == null) {
                if (release)
                    return new BytePrintStream();

                streams.put(grp, out = new BytePrintStream());
            }

            return out;
        }
    }

    /**
     * Purges print stream for this thread group to parent print stream.
     *
     * @throws IOException If any error happened.
     */
    public void purge() throws IOException {
        out(true).writeTo(out);
    }

    /**
     * Purges print stream for this thread group to the stream passed in.
     *
     * @param out Stream to purge to.
     * @throws IOException If any error happened.
     */
    public void purge(OutputStream out) throws IOException {
        out(true).writeTo(out);
    }

    /** {@inheritDoc} */
    @Override public void println() {
        out(false).println();
    }

    /** {@inheritDoc} */
    @Override public void print(char c) {
        out(false).print(c);
    }

    /** {@inheritDoc} */
    @Override public void println(char x) {
        out(false).println(x);
    }

    /** {@inheritDoc} */
    @Override public void print(double d) {
        out(false).print(d);
    }

    /** {@inheritDoc} */
    @Override public void println(double x) {
        out(false).println(x);
    }

    /** {@inheritDoc} */
    @Override public void print(float f) {
        out(false).print(f);
    }

    /** {@inheritDoc} */
    @Override public void println(float x) {
        out(false).println(x);
    }

    /** {@inheritDoc} */
    @Override public void print(int i) {
        out(false).print(i);
    }

    /** {@inheritDoc} */
    @Override public void println(int x) {
        out(false).println(x);
    }

    /** {@inheritDoc} */
    @Override public void print(long l) {
        out(false).print(l);
    }

    /** {@inheritDoc} */
    @Override public void println(long x) {
        out(false).println(x);
    }

    /** {@inheritDoc} */
    @Override public void print(boolean b) {
        out(false).print(b);
    }

    /** {@inheritDoc} */
    @Override public void println(boolean x) {
        out(false).println(x);
    }

    /** {@inheritDoc} */
    @Override public void print(char[] s) {
        out(false).print(s);
    }

    /** {@inheritDoc} */
    @Override public void println(char[] x) {
        out(false).println(x);
    }

    /** {@inheritDoc} */
    @Override public void print(Object obj) {
        out(false).print(obj);
    }

    /** {@inheritDoc} */
    @Override public void println(Object x) {
        out(false).println(x);
    }

    /** {@inheritDoc} */
    @Override public void print(String s) {
        out(false).print(s);
    }

    /** {@inheritDoc} */
    @Override public void println(String x) {
        out(false).println(x);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] buf, int off, int len) {
        out(false).write(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        out(false).close();
    }

    /** {@inheritDoc} */
    @Override public void flush() {
        out(false).flush();
    }

    /** {@inheritDoc} */
    @Override public void write(int b) {
        out(false).write(b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b) throws IOException {
        out(false).write(b);
    }

    /** {@inheritDoc} */
    @Override public PrintStream append(CharSequence csq) {
        return out(false).append(csq);
    }

    /** {@inheritDoc} */
    @Override public PrintStream append(CharSequence csq, int start, int end) {
        return out(false).append(csq, start, end);
    }

    /** {@inheritDoc} */
    @Override public PrintStream append(char c) {
        return out(false).append(c);
    }

    /** {@inheritDoc} */
    @Override public PrintStream printf(String format, Object... args) {
        return out(false).printf(format, args);
    }

    /** {@inheritDoc} */
    @Override public PrintStream printf(Locale l, String format, Object... args) {
        return out(false).printf(l, format, args);
    }

    /** {@inheritDoc} */
    @Override public PrintStream format(String format, Object... args) {
        return out(false).format(format, args);
    }

    /** {@inheritDoc} */
    @Override public PrintStream format(Locale l, String format, Object... args) {
        return out(false).format(l, format, args);
    }

    /**
     * Wrapper around print stream that allows purging to any print stream.
     */
    private static class BytePrintStream extends PrintStream {
        /**
         * Default constructor.
         */
        BytePrintStream() {
            super(new ByteArrayOutputStream());
        }

        /**
         * Writes contents of the internal byte array stream to the stream passed
         * in.
         *
         * @param out Stream to write to.
         * @throws IOException If any error happened.
         */
        void writeTo(OutputStream out) throws IOException {
            flush();

            ByteArrayOutputStream byteOut = (ByteArrayOutputStream)this.out;

            byteOut.writeTo(out);

            // Go back to beginning.
            byteOut.reset();
        }
    }
}