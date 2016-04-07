package org.apache.ignite.internal.processors.hadoop;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Error simulator.
 */
public class ErrorSimulator {

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
     * No-op instance.
     */
    public static final ErrorSimulator noopInstance = new ErrorSimulator();

    /**
     * Creates simulator of given kind with given stage bits.
     *
     * @param kind The kind.
     * @param bits The stage bits.
     * @return The simulator.
     */
    public static ErrorSimulator create(Kind kind, int bits) {
        switch (kind) {
            case Noop:
                return noopInstance;
            case Runtime:
                return new RuntimeExceptionBitErrorSimulator(bits);
            case IOException:
                return new IOExceptionBitErrorSimulator(bits);
            case Error:
                return new ErrorBitErrorSimulator(bits);
            default:
                throw new IllegalStateException("Unknown kind: " + kind);
        }
    }

    /** Instance ref. */
    private static final AtomicReference<ErrorSimulator> ref = new AtomicReference<>(noopInstance);

    /**
     * Gets instance.
     */
    public static ErrorSimulator instance() {
        return ref.get();
    }

    /**
     * Sets instance.
     */
    public static boolean setInstance(ErrorSimulator expect, ErrorSimulator update) {
        return ref.compareAndSet(expect, update);
    }

    /**
     * Constructor.
     */
    private ErrorSimulator() {
        // noop
    }

    /**
     * Invoked on the named stage.
     */
    public void onMapConfigure() {
    }

    /**
     * Invoked on the named stage.
     */
    public void onMapSetup()  throws IOException, InterruptedException {
    }

    /**
     * Invoked on the named stage.
     */
    public void onMap() throws IOException {
    }

    /**
     * Invoked on the named stage.
     */
    public void onMapCleanup()  throws IOException, InterruptedException {
    }

    /**
     * Invoked on the named stage.
     */
    public void onMapClose()  throws IOException {
    }

    /**
     * setConf() does not declare IOException to be thrown.
     */
    public void onCombineConfigure() {
    }

    /**
     * Invoked on the named stage.
     */
    public void onCombineSetup() throws IOException, InterruptedException {
    }

    /**
     * Invoked on the named stage.
     */
    public void onCombine() throws IOException {
    }

    /**
     * Invoked on the named stage.
     */
    public void onCombineCleanup() throws IOException, InterruptedException {
    }

    /**
     * Invoked on the named stage.
     */
    public void onReduceConfigure() {
    }

    /**
     * Invoked on the named stage.
     */
    public void onReduceSetup()  throws IOException, InterruptedException {
    }

    /**
     * Invoked on the named stage.
     */
    public void onReduce()  throws IOException {
    }

    /**
     * Invoked on the named stage.
     */
    public void onReduceCleanup()  throws IOException, InterruptedException {
    }


    /**
     * Runtime error simulator.
     */
    public static class RuntimeExceptionBitErrorSimulator extends ErrorSimulator {

        /** Stage bits. */
        private final int bits;

        /**
         * Constructor.
         */
        protected RuntimeExceptionBitErrorSimulator(int b) {
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
            catch (IOException e) {
                // ignore
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
            catch (IOException e) {
                // ignore
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
            catch (IOException e) {
                // ignore
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
     * Error simulator.
     */
    public static class ErrorBitErrorSimulator extends RuntimeExceptionBitErrorSimulator {
        /**
         * Constructor.
         */
        public ErrorBitErrorSimulator(int bits) {
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
    public static class IOExceptionBitErrorSimulator extends RuntimeExceptionBitErrorSimulator {
        /**
         * Constructor.
         */
        public IOExceptionBitErrorSimulator(int bits) {
            super(bits);
        }

        /** {@inheritDoc} */
        @Override protected void simulateError() throws IOException {
            throw new IOException("An error simulated by " + getClass().getSimpleName());
        }
    }
}
