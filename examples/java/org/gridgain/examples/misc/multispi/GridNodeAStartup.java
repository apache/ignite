// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.multispi;

import org.gridgain.grid.*;
import org.springframework.context.support.*;

import javax.swing.*;

/**
 * Starts up an empty node with Segment A configuration.
 * <p>
 * You can also start a stand-alone GridGain instance by passing the path
 * to configuration file to {@code 'ggstart.{sh|bat}'} script, like so:
 * {@code 'ggstart.sh path/to/nodeA.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridNodeAStartup {
    /**
     * Start up an empty node with Segment A configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        AbstractApplicationContext ctx =
            new ClassPathXmlApplicationContext("org/gridgain/examples/misc/multispi/nodeA.xml");

        // Get configuration from Spring.
        GridConfiguration cfg = ctx.getBean("grid.cfg", GridConfiguration.class);

        try (Grid g = GridGain.start(cfg);){
            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[] {
                    new JLabel("Grid node for Segment A started."),
                    new JLabel("Press OK to stop node.")
                },
                "GridGain",
                JOptionPane.INFORMATION_MESSAGE
            );
        }
    }
}
