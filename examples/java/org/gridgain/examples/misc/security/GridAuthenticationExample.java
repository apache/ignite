// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.security;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.authentication.*;

import javax.swing.*;
import java.util.*;

import static javax.swing.JOptionPane.*;

/**
 * Example that shows using of {@link GridAuthenticationSpi}. It sends broadcast
 * text message to all nodes in authentication-restricted topology. To start
 * remote node, you can run {@link GridAuthenticationNodeStartup} class.
 * <p>
 * You can also start a stand-alone GridGain instance by passing the path
 * to configuration file to {@code 'ggstart.{sh|bat}'} script, like so:
 * {@code 'ggstart.sh examples/config/example-authentication-passcode.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridAuthenticationExample {
    /** Change this property to start example in SSL mode. */
    private static final Boolean SSL_ENABLED = GridAuthenticationNodeStartup.SSL_ENABLED;

    /**
     * Executes <tt>Authentication</tt> example on the grid and sends broadcast message
     * to all nodes in the grid.
     *
     * @param args Command line arguments, none required.
     * @throws GridException In case of error.
     */
    public static void main(String[] args) throws GridException {
        // When you start remote nodes, authentication process is invoked automatically,
        // so if you see topology change events, it means that authentication succeeded.

        String cfgPath = SSL_ENABLED ? "examples/config/example-cache-ssl.xml" :
            "examples/config/example-cache-authentication-passcode.xml";

        try(Grid g = GridGain.start(cfgPath)) {
            String title = "GridGain started at " + new Date();
            String msg = "Press OK to send broadcast message, cancel to exit.";

            // Ask user to send broadcast message.
            while (confirm(title, msg)) {
                // Send notification message to all nodes in topology.
                g.compute().run(new Runnable() {
                        @Override public void run() {
                            System.out.println(">>> Broadcast message sent from node=" + g.localNode().id());
                        }
                    }
                ).get();
            }
        }
    }

    /**
     * Display confirmation dialog.
     *
     * @param title Dialog title.
     * @param msg Dialog message.
     * @return {@code true} if user presses OK button, {@code false} in all other cases.
     */
    private static boolean confirm(String title, String msg) {
        return OK_OPTION == JOptionPane.showConfirmDialog(
            null,
            new JComponent[] {
                new JLabel(title),
                new JLabel(msg)
            },
            "GridGain",
            OK_CANCEL_OPTION
        );
    }
}
