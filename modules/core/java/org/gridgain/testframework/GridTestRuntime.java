/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework;

import org.apache.commons.io.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.concurrent.atomic.*;

import static org.apache.commons.lang.SystemUtils.*;

/**
 * Runtime utility allows to execute system commands and shell scripts.
 */
public class GridTestRuntime {
    /** Spawnd processes thread's counter. */
    private static final AtomicLong CNT = new AtomicLong();

    /**
     * Start script execution in the GridGain home directory, e.g.:
     * <ul>
     *     <li>GridTestRuntime.execute("pwd")</li>
     *     <li>GridTestRuntime.execute("ls", "-la", "/")</li>
     *     <li>GridTestRuntime.execute("modules/clients-tests/bin/stop-nodes.[sh|cmd]")</li>
     * </ul>
     * <p>
     * You can use pattern [sh|cmd] or [sh|bat] to specify OS-specific script.
     *
     * @param script Script to execute.
     * @param args Optional script arguments.
     * @return Started thread linked to the started process.
     *     If you interrupt the thread, process will be destroyed.
     *     If process finishes, thread will be stopped.
     * @throws IOException If failed.
     */
    public static Thread start(OutputStream out, String script, String... args) throws IOException {
        ProcessThread t = new ProcessThread("grid-external-process-" + CNT.incrementAndGet(), out, start(script, args));

        t.setDaemon(true);
        t.start();

        return t;
    }

    /**
     * Start script execution in the GridGain home directory, e.g.:
     * <ul>
     *     <li>GridTestRuntime.execute("pwd")</li>
     *     <li>GridTestRuntime.execute("ls", "-la", "/")</li>
     *     <li>GridTestRuntime.execute("modules/clients-tests/bin/stop-nodes.[sh|cmd]")</li>
     * </ul>
     * <p>
     * You can use pattern [sh|cmd] or [sh|bat] to specify OS-specific script.
     *
     * @param script Script to execute.
     * @param args Optional script arguments.
     * @return Started process.
     * @throws IOException If failed.
     */
    public static Process start(String script, String... args) throws IOException {
        // Replace patterns [sh|cmd] and [sh|bat] with OS-specific 'sh' or 'cmd' or 'bat' extension.
        script = script.replaceAll("\\[(sh)\\|(cmd|bat)\\]$", IS_OS_WINDOWS ? "$2" : "$1");

        // Fix separators in the path.
        script = FilenameUtils.separatorsToSystem(script);

        String[] cmd;

        if (F.isEmpty(args))
            cmd = new String[] {script};
        else {
            cmd = new String[1 + args.length];

            cmd[0] = script;

            System.arraycopy(args, 0, cmd, 1, args.length);
        }

        // Execute process.
        ProcessBuilder pb = new ProcessBuilder(cmd);

        pb.directory(new File(U.getGridGainHome()));

        pb.environment().putAll(System.getenv()); // Copy all current environment variables.
        pb.environment().put("GRIDGAIN_HOME", U.getGridGainHome()); // Provide this grid instance home.

        pb.redirectErrorStream(true);

        return pb.start();
    }

    /**
     * Execute script in the GridGain home directory, e.g.:
     * <ul>
     *     <li>GridTestRuntime.execute("pwd")</li>
     *     <li>GridTestRuntime.execute("ls", "-la", "/")</li>
     *     <li>GridTestRuntime.execute("modules/clients-tests/bin/stop-nodes.[sh|cmd]")</li>
     * </ul>
     * <p>
     * You can use pattern [sh|cmd] or [sh|bat] to specify OS-specific script.
     *
     * @param script Script to execute.
     * @param args Optional script arguments.
     * @return Executed command output.
     * @throws IOException If failed.
     */
    public static ByteArrayOutputStream execute(String script, String... args) throws IOException {
        Process proc = start(script, args);

        InputStream in = proc.getInputStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        if (in != null)
            IOUtils.copy(in, out);

        try {
            int exitCode = proc.waitFor();

            if (exitCode != 0)
                throw new IOException("Failed to execute script (unexpected exit code)" +
                    " [exitCode=" + exitCode + ", script=" + script + ", out=" + out + ']');
        }
        catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();

            proc.destroy();
        }

        return out;
    }

    /**
     * Helper class to wrap external process.
     */
    private static final class ProcessThread extends Thread {
        /** Output stream to redirect process output to. */
        private final OutputStream out;

        /** Input stream from the process. */
        private final InputStream in;

        /** Process wrapped by this thread. */
        private final Process proc;

        /**
         * Constructs process-linked thread.
         *
         * @param name Thread name.
         * @param out Output stream to redirect process output to.
         * @param proc Wrapped process.
         */
        private ProcessThread(String name, OutputStream out, Process proc) {
            super(name);

            in = new BufferedInputStream(proc.getInputStream());

            this.out = out;
            this.proc = proc;
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            proc.destroy();

            try {
                in.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e); // todo: Implement.
            }

            super.interrupt();
        }


        /** {@inheritDoc} */
        @Override public void run() {
            try {
                byte[] buf = new byte[1024];

                while (!isInterrupted()) {
                    int read = in.read(buf);

                    if (read == -1) {
                        out.flush();

                        break;
                    }

                    out.write(buf, 0, read);
                }
            }
            catch (IOException e) {
                U.warn(null, "Thread was interrupted due to IO exception" +
                    " [thread=" + getName() + ", msg=" + e.getMessage() + ']');
            }
            finally {
                proc.destroy();
            }
        }
    }
}
