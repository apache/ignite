/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication.*;
import org.gridgain.grid.logger.log4j.*;
import org.gridgain.grid.marshaller.optimized.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Hadoop external process base class.
 */
public class GridHadoopExternalProcessStarter {
    /**
     * @param cmdArgs Process arguments.
     */
    public static void main(String[] cmdArgs) {
        try {
            Args args = arguments(cmdArgs);

            GridHadoopExternalCommunication comm = new GridHadoopExternalCommunication(
                args.nodeId,
                args.childProcId,
                new GridOptimizedMarshaller(),
                new GridLog4jLogger(),
                Executors.newFixedThreadPool(1),
                "test"
            );

            comm.start();

            GridHadoopProcessDescriptor nodeDesc = new GridHadoopProcessDescriptor(args.nodeId, args.parentProcId);
            nodeDesc.address(args.addr);
            nodeDesc.tcpPort(args.tcpPort);
            nodeDesc.sharedMemoryPort(args.shmemPort);


            // At this point node knows that this process has started.
            comm.sendMessage(nodeDesc, new GridHadoopProcessStartedReply());
        }
        catch (Exception e) {
            e.printStackTrace();
            // TODO.
        }
        finally {

        }
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
                case "-p": {
                    if (i == processArgs.length - 1)
                        throw new Exception("Missing process class name for '-p' parameter");

                    String processClsName = processArgs[++i];

                    args.procCls = (Class<? extends GridHadoopChildProcessBase>)
                        Class.forName(processClsName);

                    break;
                }

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
            }
        }

        // TODO validate.
        return args;
    }

    private static class Args {
        /** Process class. */
        private Class<? extends GridHadoopChildProcessBase> procCls;

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
    }
}
