/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.child;

import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.logger.java.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.util.ipc.shmem.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

/**
 * Hadoop external process base class.
 */
public class GridHadoopExternalProcessStarter {
    /** Arguments. */
    private Args args;

    /** System out. */
    private OutputStream out;

    /** System err. */
    private OutputStream err;

    /**
     * @param args Parsed arguments.
     */
    public GridHadoopExternalProcessStarter(Args args) {
        this.args = args;
    }

    /**
     * @param cmdArgs Process arguments.
     */
    public static void main(String[] cmdArgs) {
        try {
            Args args = arguments(cmdArgs);

            new GridHadoopExternalProcessStarter(args).run();
        }
        catch (Exception e) {
            System.err.println("Failed");

            System.err.println(e.getMessage());

            e.printStackTrace(System.err);
        }
    }

    /**
     *
     * @throws Exception
     */
    public void run() throws Exception {
        initializeStreams();

        ExecutorService msgExecSvc = Executors.newFixedThreadPool(1);

        GridLogger log = logger();

        GridHadoopExternalCommunication comm = new GridHadoopExternalCommunication(
            args.nodeId,
            args.childProcId,
            new GridOptimizedMarshaller(),
            log,
            msgExecSvc,
            "test"
        );

        comm.start();

        GridHadoopProcessDescriptor nodeDesc = new GridHadoopProcessDescriptor(args.nodeId, args.parentProcId);
        nodeDesc.address(args.addr);
        nodeDesc.tcpPort(args.tcpPort);
        nodeDesc.sharedMemoryPort(args.shmemPort);

        GridHadoopChildProcessRunner runner = new GridHadoopChildProcessRunner();

        runner.start(comm, nodeDesc, msgExecSvc, log);

        System.err.println("Started");
        System.err.flush();

        System.setOut(new PrintStream(out));
        System.setErr(new PrintStream(err));
    }

    /**
     * @throws Exception
     */
    private void initializeStreams() throws Exception {
        File f = new File(args.out);

        if (!f.exists()) {
            if (!f.mkdirs())
                throw new IOException("Failed to create output directory: " + args.out);
        }
        else {
            if (f.isFile())
                throw new IOException("Output directory is a file: " + args.out);
        }

        out = new FileOutputStream(new File(f, args.childProcId + ".out"));
        err = new FileOutputStream(new File(f, args.childProcId + ".err"));
    }

    /**
     * @return Logger.
     * @throws IOException If failed.
     */
    private GridLogger logger() throws IOException {
        Logger log = Logger.getLogger("");

        log.setLevel(Level.FINEST);

        for (Handler h : log.getHandlers())
            log.removeHandler(h);

        FileHandler h = new FileHandler(args.out + File.separator + args.childProcId + ".log", true);

        SimpleFormatter f = new SimpleFormatter();

        h.setFormatter(f);

        log.addHandler(h);

        Logger.getLogger(GridIpcSharedMemorySpace.class.toString()).setLevel(Level.WARNING);

        return new GridJavaLogger(log);
    }

    /**
     * @param processArgs Process arguments.
     * @return Child process instance.
     */
    private static Args arguments(String[] processArgs) throws Exception {
        Args args = new Args();

        for (int i = 0; i < processArgs.length; i++) {
            String arg = processArgs[i];

            switch (arg) {
                case "-cpid": {
                    if (i == processArgs.length - 1)
                        throw new Exception("Missing process ID for '-cpid' parameter");

                    String procIdStr = processArgs[++i];

                    args.childProcId = UUID.fromString(procIdStr);

                    break;
                }

                case "-ppid": {
                    if (i == processArgs.length - 1)
                        throw new Exception("Missing process ID for '-ppid' parameter");

                    String procIdStr = processArgs[++i];

                    args.parentProcId = UUID.fromString(procIdStr);

                    break;
                }

                case "-nid": {
                    if (i == processArgs.length - 1)
                        throw new Exception("Missing node ID for '-nid' parameter");

                    String nodeIdStr = processArgs[++i];

                    args.nodeId = UUID.fromString(nodeIdStr);

                    break;
                }

                case "-addr": {
                    if (i == processArgs.length - 1)
                        throw new Exception("Missing node address for '-addr' parameter");

                    args.addr = processArgs[++i];

                    break;
                }

                case "-tport": {
                    if (i == processArgs.length - 1)
                        throw new Exception("Missing tcp port for '-tport' parameter");

                    args.tcpPort = Integer.parseInt(processArgs[++i]);

                    break;
                }

                case "-sport": {
                    if (i == processArgs.length - 1)
                        throw new Exception("Missing shared memory port for '-sport' parameter");

                    args.shmemPort = Integer.parseInt(processArgs[++i]);

                    break;
                }

                case "-out": {
                    if (i == processArgs.length - 1)
                        throw new Exception("Missing output folder name for '-out' parameter");

                    args.out = processArgs[++i];

                    break;
                }
            }
        }

        // TODO validate.
        return args;
    }

    /**
     * Execution arguments.
     */
    private static class Args {
        /** Process ID. */
        private UUID childProcId;

        /** Process ID. */
        private UUID parentProcId;

        /** Process ID. */
        private UUID nodeId;

        /** Node address. */
        private String addr;

        /** TCP port */
        private int tcpPort;

        /** Shmem port. */
        private int shmemPort = -1;

        /** Output folder. */
        private String out;
    }
}
