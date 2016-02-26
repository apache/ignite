package org.apache.ignite.internal.processors.hadoop;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import static java.lang.System.out;

/**
 *
 */
public class ErrorSimulator {

    public static ErrorSimulator noopInstance = new ErrorSimulator();

    private static final AtomicReference<ErrorSimulator> ref = new AtomicReference<>(noopInstance);

    public static ErrorSimulator instance() {
        return ref.get();
    }

    public static boolean setInstance(ErrorSimulator expect, ErrorSimulator update) {
        return ref.compareAndSet(expect, update);
    }


    protected ErrorSimulator() {
        // noop
    }


    public void onMapConfigure() {
        //out.println("# map configure");
    }

    public void onMapSetup()  throws IOException, InterruptedException {
        //out.println("# map setup");
    }

    public void onMap() throws IOException {
        //out.println("# map");
    }

    public void onMapCleanup()  throws IOException, InterruptedException {
        //out.println("# map cleanup");
    }


    public void onMapClose()  throws IOException {
        //out.println("# map close");
    }


    public void onCombineConfigure() {
        //out.println("# combine configure ");
    }

    public void onCombineSetup() throws IOException, InterruptedException {
        //out.println("# combine setup ");
    }

    public void onCombine() throws IOException {
        //out.println("# combine reduce ");
    }

    public void onCombineCleanup() throws IOException, InterruptedException {
        //out.println("# combine cleanup ");
    }



    public void onReduceConfigure() {
        //out.println("# reduce configure ");
    }

    public void onReduceSetup()  throws IOException, InterruptedException {
        //out.println("# reduce setup ");
    }

    public void onReduce()  throws IOException {
        //out.println("# reduce ");
    }

    public void onReduceCleanup()  throws IOException, InterruptedException {
        //out.println("# reduce cleanup ");
    }


    public static class RuntimeExceptionBitErrorSimulator extends ErrorSimulator {

        private final int bits;

        RuntimeExceptionBitErrorSimulator(int b) {
            bits = b;
        }

        protected void simulateError() throws IOException {
            throw new RuntimeException("An error simulated by " + getClass().getSimpleName());
        }

        @Override public void onMapConfigure() {
            try {
                if ((bits & 1) != 0)
                    simulateError();
            }
            catch (IOException e) {
                // ignore
            }
        }

        @Override public void onMapSetup() throws IOException, InterruptedException {
            if ((bits & 2) != 0)
                simulateError();
        }

        @Override public void onMap() throws IOException {
            if ((bits & 4) != 0)
                simulateError();
        }

        @Override public void onMapCleanup() throws IOException, InterruptedException {
            if ((bits & 8) != 0)
                simulateError();
        }



        @Override public void onCombineConfigure() {
            try {
                if ((bits & 16) != 0)
                    simulateError();
            }
            catch (IOException e) {
                // ignore
            }
        }

        @Override public void onCombineSetup() throws IOException, InterruptedException {
            if ((bits & 32) != 0)
                simulateError();
        }

        @Override public void onCombine() throws IOException {
            if ((bits & 64) != 0)
                simulateError();
        }

        @Override public void onCombineCleanup() throws IOException, InterruptedException {
            if ((bits & 128) != 0)
                simulateError();
        }



        @Override public void onReduceConfigure() {
            try {
                if ((bits & 256) != 0)
                    simulateError();
            }
            catch (IOException e) {
                // ignore
            }
        }

        @Override public void onReduceSetup() throws IOException, InterruptedException {
            if ((bits & 512) != 0)
                simulateError();
        }

        @Override public void onReduce() throws IOException {
            if ((bits & 1024) != 0)
                simulateError();
        }

        @Override public void onReduceCleanup() throws IOException, InterruptedException {
            if ((bits & 2048) != 0)
                simulateError();
        }
    }

    public static class ErrorBitErrorSimulator extends RuntimeExceptionBitErrorSimulator {
        public ErrorBitErrorSimulator(int bits) {
            super(bits);
        }

        @Override protected void simulateError() {
            throw new Error("An error simulated by " + getClass().getSimpleName());
        }
    }

    public static class IOExceptionBitErrorSimulator extends RuntimeExceptionBitErrorSimulator {
        public IOExceptionBitErrorSimulator(int bits) {
            super(bits);
        }

        @Override protected void simulateError() throws IOException {
            throw new IOException("An error simulated by " + getClass().getSimpleName());
        }
    }
}
