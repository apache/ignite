/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication.*;
import org.gridgain.grid.util.nio.*;

import java.lang.reflect.*;
import java.net.*;

/**
 * Hadoop external process base class.
 */
public class GridHadoopExternalProcessStarter {
    /**
     * Supported arguments parameters:
     * <li>
     *     <ul> -p Process implementation class name.
     * </li>
     *
     * @param args Process arguments.
     */
    public static void main(String[] args) {
        try {
            GridHadoopChildProcessBase process = createProcess(args);

            GridHadoopExternalCommunication comm = new GridHadoopExternalCommunication(process.listener());

            // TODO wrap around start port.
            for (int port = 20000; port < 21000; port++) {
                try {
                    comm.start(port);

                    System.out.println(port);
                    System.out.flush();

                    // Override process output streams.
                    System.setOut();
                    System.setErr();
                }
                catch (GridNioBindException ignored) {
                    // Continue to next port.
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            // TODO.
        }
    }

    /**
     * @param processArgs Process arguments.
     * @return Child process instance.
     */
    private static GridHadoopChildProcessBase createProcess(String[] processArgs) throws Exception {
        for (int i = 0; i < processArgs.length; i++) {
            String arg = processArgs[i];

            if ("-p".equals(arg)) {
                if (i == processArgs.length - 1)
                    throw new Exception("Missing process class name for '-p' parameter");

                String processClsName = processArgs[i + 1];

                Class<?> cls = Class.forName(processClsName);

                Constructor<?> ctor = cls.getConstructor(String[].class);

                return (GridHadoopChildProcessBase)ctor.newInstance(processArgs);
            }
        }

        throw new Exception("Failed to find process class name in arguments.");
    }
}
