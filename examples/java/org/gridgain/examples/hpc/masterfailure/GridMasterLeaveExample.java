// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.masterfailure;

import org.gridgain.grid.*;

import javax.swing.*;

/**
 * This example demonstrates how {@link org.gridgain.grid.compute.GridComputeJobMasterLeaveAware} callback works.
 * <p>
 * In order to run this example you should start a remote worker node using {@link GridMasterLeaveNodeStartup}
 * class.
 * <p>
 * When you launch the example, a master node is started and {@link GridMasterLeaveJob} is sent to the remote
 * worker node.
 * <p>
 * Then you will be asked to stop this node. Once it is done, the remote worker will invoke
 * {@code GridComputeJobMasterLeaveAware} callback on its running job. This callback will save intermediate job state
 * as a global checkpoint.
 * <p>
 * Finally, execute this example again and check worker node's output. You will see that {@code GridMasterLeaveJob}
 * is started again and this time counting starts not from {@code 0}, but from its previously saved
 * intermediate state.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridMasterLeaveExample {
    /**
     * Synthetic job key which is used in order to distinguish one job from another when preserving state.
     * It helps us make sure that we only read the saved checkpoint in case of task failure, not in case of
     * new job being started.
     */
    public static final String USER_JOB_ID = "usr-job-id";

    /**
     * Execute the example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        // Start new grid instance which will be used as a master node.
        try (Grid g = GridGain.start("examples/config/example-master-leave-aware.xml")) {
            if (g.nodes().size() < 2)
                System.err.println(">>> No remote nodes found. Please start a remote node using GridMasterLeaveNodeStartup " +
                    "class.");
            else {
                // Start GridMasterLeaveTask execution on worker node.
                System.out.println(">>> Starting GridMasterLeaveJob on remote node ...");

                g.forRemotes().compute().call(new GridMasterLeaveJob(USER_JOB_ID));

                // Show pop-up asking to stop the master.
                System.out.println(">>> Please stop this node and check worker node's output.");
                System.out.println(">>> Restart this example and observe the job restarted from the saved checkpoint state.");

                JOptionPane.showMessageDialog(
                    null,
                    new JComponent[]{
                        new JLabel("Master node started."),
                        new JLabel("Press OK to stop it."),
                        new JLabel("Restart it again if needed to complete example.")
                    },
                    "GridGain Master Node",
                    JOptionPane.INFORMATION_MESSAGE
                );
            }
        }
    }
}
