// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.security;

import org.gridgain.grid.*;

import javax.swing.*;

import static javax.swing.JOptionPane.*;

/**
 * Starts up an empty node with passcode-based authentication configuration.
 * You can also start a stand-alone GridGain instance by passing the path
 * to configuration file to {@code 'ggstart.{sh|bat}'} script, like so:
 * {@code 'ggstart.sh examples/config/example-authentication-passcode.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridAuthenticationNodeStartup {
    /** Change this property to start node in SSL mode. */
    static final Boolean SSL_ENABLED = false;

    /**
     * Start up an empty node with specified authentication configuration.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Grid g = GridGain.start(SSL_ENABLED ? "examples/config/example-cache-ssl.xml" :
            "examples/config/example-cache-authentication-passcode.xml"))
        {
            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[] {
                    new JLabel("GridGain started."),
                    new JLabel("Press OK to stop GridGain.")
                },
                "GridGain",
                INFORMATION_MESSAGE
            );
        }
        catch (GridException e) {
            System.out.println("Failed to create grid: " + e.getMessage());
        }
    }
}
