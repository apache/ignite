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
 * Starts a worker node for {@link GridMasterLeaveExample}.
 * <p>
 * At first this node will wait for some remote node to join topology and execute {@link GridMasterLeaveJob}.
 * <p>
 * Once task execution is initiated we wait for master node to leave the topology. When it happened,
 * we re-execute the same task, this time on worker node.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridMasterLeaveNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-master-leave-aware.xml")) {
            // Wait for master node to join.
            System.out.println(">>> Waiting for master node to join topology and execute GridMasterLeaveJob ...");

            JOptionPane.showMessageDialog(
                null,
                new JComponent[]{
                    new JLabel("Worker node started."),
                    new JLabel("Press OK to stop it.")
                },
                "GridGain Worker Node",
                JOptionPane.INFORMATION_MESSAGE
            );
        }
    }
}
