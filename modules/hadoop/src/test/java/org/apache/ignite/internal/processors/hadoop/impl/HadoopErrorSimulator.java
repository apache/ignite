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

package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Error simulator.
 */
public class HadoopErrorSimulator {
    /** No-op singleton instance. */
    public static final HadoopErrorSimulator noopInstance = new HadoopErrorSimulator();

    /** Instance ref. */
    private static final AtomicReference<HadoopErrorSimulator> ref = new AtomicReference<>(noopInstance);

    /**
     * Creates simulator of given kind with given stage bits.
     *
     * @param kind The kind.
     * @param bits The stage bits.
     * @return The simulator.
     */
    public static HadoopErrorSimulator create(Kind kind, int bits) {
        switch (kind) {
            case Noop:
                return noopInstance;
            case Runtime:
                return new RuntimeExceptionBitHadoopErrorSimulator(bits);
            case IOException:
                return new IOExceptionBitHadoopErrorSimulator(bits);
            case Error:
                return new ErrorBitHadoopErrorSimulator(bits);
            default:
                throw new IllegalStateException("Unknown kind: " + kind);
        }
    }

    /**
     * Gets the error simulator instance.
     */
    public static HadoopErrorSimulator instance() {
        return ref.get();
    }

    /**
     * Sets instance.
     */
    public static boolean setInstance(HadoopErrorSimulator expect, HadoopErrorSimulator update) {
        return ref.compareAndSet(expect, update);
    }

    /**
     * Constructor.
     */
    private HadoopErrorSimulator() {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onMapConfigure() {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onMapSetup()  throws IOException, InterruptedException {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onMap() throws IOException {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onMapCleanup()  throws IOException, InterruptedException {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onMapClose()  throws IOException {
        // no-op
    }

    /**
     * setConf() does not declare IOException to be thrown.
     */
    public void onCombineConfigure() {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onCombineSetup() throws IOException, InterruptedException {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onCombine() throws IOException {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onCombineCleanup() throws IOException, InterruptedException {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onReduceConfigure() {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onReduceSetup()  throws IOException, InterruptedException {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onReduce()  throws IOException {
        // no-op
    }

    /**
     * Invoked on the named stage.
     */
    public void onReduceCleanup()  throws IOException, InterruptedException {
        // no-op
    }

    /**
     * Error kind.
     */
    public enum Kind {
        /** No error. */
        Noop,

        /** Runtime. */
        Runtime,

        /** IOException. */
        IOException,

        /** java.lang.Error. */
        Error
    }

    /**
     * Runtime error simulator.
     */
    public static class RuntimeExceptionBitHadoopErrorSimulator extends HadoopErrorSimulator {
        /** Stage bits: defines what map-reduce stages will cause errors. */
        private final int bits;

        /**
         * Constructor.
         */
        protected RuntimeExceptionBitHadoopErrorSimulator(int b) {
            bits = b;
        }

        /**
         * Simulates an error.
         */
        protected void simulateError() throws IOException {
            throw new RuntimeException("An error simulated by " + getClass().getSimpleName());
        }

        /** {@inheritDoc} */
        @Override public final void onMapConfigure() {
            try {
                if ((bits & 1) != 0)
                    simulateError();
            }
            catch (IOException ignored) {
                // No-op.
            }
        }

        /** {@inheritDoc} */
        @Override public final void onMapSetup() throws IOException, InterruptedException {
            if ((bits & 2) != 0)
                simulateError();
        }

        /** {@inheritDoc} */
        @Override public final void onMap() throws IOException {
            if ((bits & 4) != 0)
                simulateError();
        }

        /** {@inheritDoc} */
        @Override public final void onMapCleanup() throws IOException, InterruptedException {
            if ((bits & 8) != 0)
                simulateError();
        }

        /** {@inheritDoc} */
        @Override public final void onCombineConfigure() {
            try {
                if ((bits & 16) != 0)
                    simulateError();
            }
            catch (IOException ignored) {
                // No-op.
            }
        }

        /** {@inheritDoc} */
        @Override public final void onCombineSetup() throws IOException, InterruptedException {
            if ((bits & 32) != 0)
                simulateError();
        }

        /** {@inheritDoc} */
        @Override public final void onCombine() throws IOException {
            if ((bits & 64) != 0)
                simulateError();
        }

        /** {@inheritDoc} */
        @Override public final void onCombineCleanup() throws IOException, InterruptedException {
            if ((bits & 128) != 0)
                simulateError();
        }

        /** {@inheritDoc} */
        @Override public final void onReduceConfigure() {
            try {
                if ((bits & 256) != 0)
                    simulateError();
            }
            catch (IOException ignored) {
                // No-op.
            }
        }

        /** {@inheritDoc} */
        @Override public final void onReduceSetup() throws IOException, InterruptedException {
            if ((bits & 512) != 0)
                simulateError();
        }

        /** {@inheritDoc} */
        @Override public final void onReduce() throws IOException {
            if ((bits & 1024) != 0)
                simulateError();
        }

        /** {@inheritDoc} */
        @Override public final void onReduceCleanup() throws IOException, InterruptedException {
            if ((bits & 2048) != 0)
                simulateError();
        }
    }

    /**
     * java.lang.Error simulator.
     */
    public static class ErrorBitHadoopErrorSimulator extends RuntimeExceptionBitHadoopErrorSimulator {
        /**
         * Constructor.
         */
        public ErrorBitHadoopErrorSimulator(int bits) {
            super(bits);
        }

        /** {@inheritDoc} */
        @Override protected void simulateError() {
            throw new Error("An error simulated by " + getClass().getSimpleName());
        }
    }

    /**
     * IOException simulator.
     */
    public static class IOExceptionBitHadoopErrorSimulator extends RuntimeExceptionBitHadoopErrorSimulator {
        /**
         * Constructor.
         */
        public IOExceptionBitHadoopErrorSimulator(int bits) {
            super(bits);
        }

        /** {@inheritDoc} */
        @Override protected void simulateError() throws IOException {
            throw new IOException("An IOException simulated by " + getClass().getSimpleName());
        }
    }
}
