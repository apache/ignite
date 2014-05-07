/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * External process registry. Handles external process lifecycle.
 */
public class GridHadoopExternalProcessRegistry {
    /** Local node ID. */
    private UUID locNodeId;

    /** Hadoop context. */
    private GridHadoopContext ctx;

    /** */
    private String javaCmd;

    /** Logger. */
    private GridLogger log;

    /** Starting processes. */
    private ConcurrentMap<UUID, GridFutureAdapter<GridHadoopProcessDescriptor>> startingProcs =
        new ConcurrentHashMap8<>();

    public GridHadoopExternalProcessRegistry(GridHadoopContext ctx) {
        this.ctx = ctx;

        log = ctx.kernalContext().log(GridHadoopExternalProcessRegistry.class);
    }

    /**
     * @throws GridException If initialization failed.
     */
    public void start() throws GridException {
        initJavaCommand();
    }

    public GridFuture<GridHadoopProcessDescriptor> startProcess(GridHadoopExternalProcessMetadata meta) {
        UUID childProcId = UUID.randomUUID();

        GridFutureAdapter<GridHadoopProcessDescriptor> fut = new GridFutureAdapter<>(ctx.kernalContext());

        GridFutureAdapter<GridHadoopProcessDescriptor> old = startingProcs.put(childProcId, fut);

        assert old == null;

        Process proc = initializeProcess(childProcId, meta);

        // TODO verify error?

        return null;
    }

    private void initJavaCommand() throws GridException {
        String javaHome = System.getProperty("java.home");

        if (javaHome == null)
            javaHome = System.getenv("JAVA_HOME");

        if (javaHome == null)
            throw new GridException("Failed to locate JAVA_HOME.");

        javaCmd = javaHome + File.separator + "bin" + File.separator + (U.isWindows() ? "java.exe" : "java");

        try {
            Process proc = new ProcessBuilder(javaCmd, "-version").redirectErrorStream(true).start();

            Collection<String> out = readProcessOutput(proc);

            int res = proc.waitFor();

            if (res != 0)
                throw new GridException("Failed to execute 'java -version' command (process finished with nonzero " +
                    "code) [exitCode=" + res + ", javaCmd='" + javaCmd + "', msg=" + F.first(out) + ']');

            if (log.isInfoEnabled()) {
                log.info("Will use java for external task execution: ");

                for (String s : out)
                    log.info("    " + s);
            }
        }
        catch (IOException e) {
            throw new GridException("Failed to check java for external task execution.", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridException("Failed to wait for process completion (thread got interrupted).", e);
        }
    }

    /**
     * Reads process output line-by-line.
     *
     * @param proc Process to read output.
     * @return Read lines.
     * @throws IOException If read failed.
     */
    private Collection<String> readProcessOutput(Process proc) throws IOException {
        BufferedReader rdr = new BufferedReader(new InputStreamReader(proc.getInputStream()));

        Collection<String> res = new ArrayList<>();

        String s;

        while ((s = rdr.readLine()) != null)
            res.add(s);

        return res;
    }

    /**
     * Builds process from metadata.
     *
     * @param childProcId Child process ID.
     * @param meta Metadata.
     * @return Started process.
     */
    private Process initializeProcess(UUID childProcId, GridHadoopExternalProcessMetadata meta) {
        return null;
    }
}
