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

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external.child;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopProcessDescriptor;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication.HadoopExternalCommunication;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

/**
 * Hadoop external process base class.
 */
public class HadoopExternalProcessStarter {
    /** Path to Log4j configuration file. */
    public static final String DFLT_LOG4J_CONFIG = "config/ignite-log4j.xml";

    /** Arguments. */
    private Args args;

    /** System out. */
    private OutputStream out;

    /** System err. */
    private OutputStream err;

    /**
     * @param args Parsed arguments.
     */
    public HadoopExternalProcessStarter(Args args) {
        this.args = args;
    }

    /**
     * @param cmdArgs Process arguments.
     */
    public static void main(String[] cmdArgs) {
        try {
            Args args = arguments(cmdArgs);

            new HadoopExternalProcessStarter(args).run();
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
        U.setWorkDirectory(args.workDir, U.getIgniteHome());

        File outputDir = outputDirectory();

        initializeStreams(outputDir);

        ExecutorService msgExecSvc = Executors.newFixedThreadPool(
            Integer.getInteger("MSG_THREAD_POOL_SIZE", Runtime.getRuntime().availableProcessors() * 2));

        IgniteLogger log = logger(outputDir);

        HadoopExternalCommunication comm = new HadoopExternalCommunication(
            args.nodeId,
            args.childProcId,
            new JdkMarshaller(),
            log,
            msgExecSvc,
            "external"
        );

        comm.start();

        HadoopProcessDescriptor nodeDesc = new HadoopProcessDescriptor(args.nodeId, args.parentProcId);
        nodeDesc.address(args.addr);
        nodeDesc.tcpPort(args.tcpPort);
        nodeDesc.sharedMemoryPort(args.shmemPort);

        HadoopChildProcessRunner runner = new HadoopChildProcessRunner();

        runner.start(comm, nodeDesc, msgExecSvc, log);

        System.err.println("Started");
        System.err.flush();

        System.setOut(new PrintStream(out));
        System.setErr(new PrintStream(err));
    }

    /**
     * @param outputDir Directory for process output.
     * @throws Exception
     */
    private void initializeStreams(File outputDir) throws Exception {
        out = new FileOutputStream(new File(outputDir, args.childProcId + ".out"));
        err = new FileOutputStream(new File(outputDir, args.childProcId + ".err"));
    }

    /**
     * @return Path to output directory.
     * @throws IOException If failed.
     */
    private File outputDirectory() throws IOException {
        File f = new File(args.out);

        if (!f.exists()) {
            if (!f.mkdirs())
                throw new IOException("Failed to create output directory: " + args.out);
        }
        else {
            if (f.isFile())
                throw new IOException("Output directory is a file: " + args.out);
        }

        return f;
    }

    /**
     * @param outputDir Directory for process output.
     * @return Logger.
     */
    private IgniteLogger logger(final File outputDir) {
        final URL url = U.resolveIgniteUrl(DFLT_LOG4J_CONFIG);

        Log4JLogger logger;

        try {
            logger = url != null ? new Log4JLogger(url) : new Log4JLogger(true);
        }
        catch (IgniteCheckedException e) {
            System.err.println("Failed to create URL-based logger. Will use default one.");

            e.printStackTrace();

            logger = new Log4JLogger(true);
        }

        logger.updateFilePath(new IgniteClosure<String, String>() {
            @Override public String apply(String s) {
                return new File(outputDir, args.childProcId + ".log").getAbsolutePath();
            }
        });

        return logger;
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

                case "-wd": {
                    if (i == processArgs.length - 1)
                        throw new Exception("Missing work folder name for '-wd' parameter");

                    args.workDir = processArgs[++i];

                    break;
                }
            }
        }

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

        /** Work directory. */
        private String workDir;
    }
}