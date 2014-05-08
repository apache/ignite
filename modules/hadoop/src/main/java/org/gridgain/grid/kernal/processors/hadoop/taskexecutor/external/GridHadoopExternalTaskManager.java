/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.child.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * External process registry. Handles external process lifecycle.
 */
public class GridHadoopExternalTaskManager {
    /** Hadoop context. */
    private GridHadoopContext ctx;

    /** */
    private String javaCmd;

    /** Logger. */
    private GridLogger log;

    /** Node process descriptor. */
    private GridHadoopProcessDescriptor nodeDesc;

    /** Output base. */
    private String outputBase;

    /** Path separator. */
    private String pathSep;

    /** Small executor service to build processes asynchronously. */
    private ExecutorService startExec;

    /** Hadoop external communication. */
    private GridHadoopExternalCommunication comm;

    /** Starting processes. */
    private ConcurrentMap<UUID, GridHadoopProcessFuture> startingProcs =
        new ConcurrentHashMap8<>();

    /** Starting processes. */
    private ConcurrentMap<UUID, Process> runningProcs =
        new ConcurrentHashMap8<>();

    public GridHadoopExternalTaskManager(GridHadoopContext ctx) {
        this.ctx = ctx;

        log = ctx.kernalContext().log(GridHadoopExternalTaskManager.class);

        outputBase = U.getGridGainHome() + File.separator + "work" + File.separator + "hadoop";

        startExec = Executors.newFixedThreadPool(4); // TODO.

        pathSep = System.getProperty("path.separator", U.isWindows() ? ";" : ":");
    }

    /**
     * @throws GridException If initialization failed.
     */
    public void start() throws GridException {
        initJavaCommand();

        comm = new GridHadoopExternalCommunication(
            ctx.localNodeId(),
            UUID.randomUUID(),
            ctx.kernalContext().config().getMarshaller(),
            log,
            ctx.kernalContext().config().getSystemExecutorService(),
            ctx.kernalContext().gridName());

        comm.setListener(new MessageListener());

        comm.start();

        nodeDesc = comm.localProcessDescriptor();

        ctx.kernalContext().ports().registerPort(nodeDesc.tcpPort(), GridPortProtocol.TCP,
            GridHadoopExternalTaskManager.class);

        if (nodeDesc.sharedMemoryPort() != -1)
            ctx.kernalContext().ports().registerPort(nodeDesc.sharedMemoryPort(), GridPortProtocol.TCP,
                GridHadoopExternalTaskManager.class);
    }

    public void stop() throws GridException {
        comm.stop();

        startExec.shutdown();

        try {
            startExec.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInternalException(e);
        }
    }

    public GridFuture<GridHadoopProcessDescriptor> startProcess(final GridHadoopExternalTaskMetadata meta) {
        final UUID childProcId = UUID.randomUUID();

        final GridHadoopProcessFuture fut = new GridHadoopProcessFuture(childProcId, ctx.kernalContext());

        GridHadoopProcessFuture old = startingProcs.put(childProcId, fut);

        assert old == null;

        startExec.submit(new Runnable() {
            @Override public void run() {
                try {
                    Process proc = initializeProcess(childProcId, meta);

                    BufferedReader rdr = new BufferedReader(new InputStreamReader(proc.getInputStream()));

                    String line;

                    // Read up all the process output.
                    while ((line = rdr.readLine()) != null) {
                        if ("Started".equals(line)) {
                            // Process started successfully, it should not write anything more to the output stream.
                            if (log.isDebugEnabled())
                                log.debug("Successfully started child process [childProcId=" + childProcId +
                                    ", meta=" + meta + ']');

                            Process old = runningProcs.put(childProcId, proc);

                            assert old == null;

                            fut.onProcessStarted();
                        }
                        else if ("Failed".equals(line)) {
                            StringBuilder sb = new StringBuilder("Failed to start child process: " + meta + "\n");

                            while ((line = rdr.readLine()) != null)
                                sb.append("    ").append(line).append("\n");

                            // Cut last character.
                            sb.setLength(sb.length() - 1);

                            log.warning(sb.toString());

                            fut.onDone(new GridException(sb.toString()));

                            break;
                        }
                    }
                }
                catch (Exception e) {
                    fut.onDone(new GridException("Failed to initialize child process: " + meta, e));
                }
            }
        });

        return fut;
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
    private Process initializeProcess(UUID childProcId, GridHadoopExternalTaskMetadata meta) throws Exception {
        List<String> cmd = new ArrayList<>();

        cmd.add(javaCmd);
        cmd.addAll(meta.jvmOptions());
        cmd.add("-cp");
        cmd.add(buildClasspath(meta.classpath()));
        cmd.add(GridHadoopExternalProcessStarter.class.getName());
        cmd.add("-cpid");
        cmd.add(String.valueOf(childProcId));
        cmd.add("-ppid");
        cmd.add(String.valueOf(nodeDesc.processId()));
        cmd.add("-nid");
        cmd.add(String.valueOf(nodeDesc.parentNodeId()));
        cmd.add("-addr");
        cmd.add(nodeDesc.address());
        cmd.add("-tport");
        cmd.add(String.valueOf(nodeDesc.tcpPort()));
        cmd.add("-sport");
        cmd.add(String.valueOf(nodeDesc.sharedMemoryPort()));
        cmd.add("-out");
        cmd.add(outputBase + File.separator + childProcId);

        return new ProcessBuilder(cmd)
            .redirectErrorStream(true)
            .start();
    }

    /**
     * @param cp Classpath collection.
     * @return Classpath string.
     */
    private String buildClasspath(Collection<String> cp) {
        assert !cp.isEmpty();

        StringBuilder sb = new StringBuilder();

        for (String s : cp)
            sb.append(s).append(pathSep);

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    /**
     *
     */
    private class MessageListener implements GridHadoopMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridHadoopProcessDescriptor desc, GridHadoopMessage msg) {
            if (msg instanceof GridHadoopProcessStartedReply) {
                GridHadoopProcessFuture fut = startingProcs.get(desc.processId());

                if (fut != null)
                    fut.onReplyReceived(desc);
                // Safety.
                else
                    log.warning("Failed to find process start future (will ignore): " + desc);
            }
        }

        /** {@inheritDoc} */
        @Override public void onConnectionLost(GridHadoopProcessDescriptor desc) {
            // Cleanup. Gracefully terminated process should be cleaned up by this moment.
            Process proc = runningProcs.remove(desc.processId());

            if (proc != null) {
                log.warning("Lost connection with alive process (will terminate): " + desc);

                proc.destroy();
            }
        }
    }

    private class GridHadoopProcessFuture extends GridFutureAdapter<GridHadoopProcessDescriptor> {
        /** Child process ID. */
        private UUID childProcId;

        /** Process descriptor. */
        private GridHadoopProcessDescriptor desc;

        /** Process started flag. */
        private volatile boolean procStarted;

        /** Reply received flag. */
        private volatile boolean replyReceived;

        /**
         * Empty constructor.
         */
        public GridHadoopProcessFuture() {
            // No-op.
        }

        /**
         * @param ctx Kernal context.
         */
        private GridHadoopProcessFuture(UUID childProcId, GridKernalContext ctx) {
            super(ctx);

            this.childProcId = childProcId;
        }

        /**
         * Process started callback.
         */
        public void onProcessStarted() {
            procStarted = true;

            if (procStarted && replyReceived)
                onDone(desc);
        }

        /**
         * Reply received callback.
         */
        public void onReplyReceived(GridHadoopProcessDescriptor desc) {
            assert childProcId.equals(desc.processId());

            this.desc = desc;

            replyReceived = true;

            if (procStarted && replyReceived)
                onDone(desc);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable GridHadoopProcessDescriptor res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                startingProcs.remove(childProcId, this);

                return true;
            }

            return false;
        }
    }
}
