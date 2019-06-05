/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.h2.util.Tool;

/**
 * Creates a cluster from a stand-alone database.
 * <br />
 * Copies a database to another location if required.
 * @h2.resource
 */
public class CreateCluster extends Tool {

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-urlSource "&lt;url&gt;"]</td>
     * <td>The database URL of the source database (jdbc:h2:...)</td></tr>
     * <tr><td>[-urlTarget "&lt;url&gt;"]</td>
     * <td>The database URL of the target database (jdbc:h2:...)</td></tr>
     * <tr><td>[-user &lt;user&gt;]</td>
     * <td>The user name (default: sa)</td></tr>
     * <tr><td>[-password &lt;pwd&gt;]</td>
     * <td>The password</td></tr>
     * <tr><td>[-serverList &lt;list&gt;]</td>
     * <td>The comma separated list of host names or IP addresses</td></tr>
     * </table>
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new CreateCluster().runTool(args);
    }

    @Override
    public void runTool(String... args) throws SQLException {
        String urlSource = null;
        String urlTarget = null;
        String user = "";
        String password = "";
        String serverList = null;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-urlSource")) {
                urlSource = args[++i];
            } else if (arg.equals("-urlTarget")) {
                urlTarget = args[++i];
            } else if (arg.equals("-user")) {
                user = args[++i];
            } else if (arg.equals("-password")) {
                password = args[++i];
            } else if (arg.equals("-serverList")) {
                serverList = args[++i];
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        if (urlSource == null || urlTarget == null || serverList == null) {
            showUsage();
            throw new SQLException("Source URL, target URL, or server list not set");
        }
        process(urlSource, urlTarget, user, password, serverList);
    }

    /**
     * Creates a cluster.
     *
     * @param urlSource the database URL of the original database
     * @param urlTarget the database URL of the copy
     * @param user the user name
     * @param password the password
     * @param serverList the server list
     */
    public void execute(String urlSource, String urlTarget,
            String user, String password, String serverList) throws SQLException {
        process(urlSource, urlTarget, user, password, serverList);
    }

    private static void process(String urlSource, String urlTarget,
            String user, String password, String serverList) throws SQLException {
        org.h2.Driver.load();

        // use cluster='' so connecting is possible
        // even if the cluster is enabled
        try (Connection connSource = DriverManager.getConnection(urlSource + ";CLUSTER=''", user, password);
                Statement statSource = connSource.createStatement()) {
            // enable the exclusive mode and close other connections,
            // so that data can't change while restoring the second database
            statSource.execute("SET EXCLUSIVE 2");
            try {
                performTransfer(statSource, urlTarget, user, password, serverList);
            } finally {
                // switch back to the regular mode
                statSource.execute("SET EXCLUSIVE FALSE");
            }
        }
    }

    private static void performTransfer(Statement statSource, String urlTarget, String user, String password,
            String serverList) throws SQLException {

        // Delete the target database first.
        try (Connection connTarget = DriverManager.getConnection(urlTarget + ";CLUSTER=''", user, password);
                Statement statTarget = connTarget.createStatement()) {
            statTarget.execute("DROP ALL OBJECTS DELETE FILES");
        }

        try (PipedReader pipeReader = new PipedReader()) {
            Future<?> threadFuture = startWriter(pipeReader, statSource);

            // Read data from pipe reader, restore on target.
            try (Connection connTarget = DriverManager.getConnection(urlTarget, user, password);
                    Statement statTarget = connTarget.createStatement()) {
                RunScript.execute(connTarget, pipeReader);

                // Check if the writer encountered any exception
                try {
                    threadFuture.get();
                } catch (ExecutionException ex) {
                    throw new SQLException(ex.getCause());
                } catch (InterruptedException ex) {
                    throw new SQLException(ex);
                }

                // set the cluster to the serverList on both databases
                statSource.executeUpdate("SET CLUSTER '" + serverList + "'");
                statTarget.executeUpdate("SET CLUSTER '" + serverList + "'");
            }
        } catch (IOException ex) {
            throw new SQLException(ex);
        }
    }

    private static Future<?> startWriter(final PipedReader pipeReader,
            final Statement statSource) {
        final ExecutorService thread = Executors.newFixedThreadPool(1);

        // Since exceptions cannot be thrown across thread boundaries, return
        // the task's future so we can check manually
        Future<?> threadFuture = thread.submit(new Runnable() {
            @Override
            public void run() {
                // If the creation of the piped writer fails, the reader will
                // throw an IOException as soon as read() is called: IOException
                // - if the pipe is broken, unconnected, closed, or an I/O error
                // occurs. The reader's IOException will then trigger the
                // finally{} that releases exclusive mode on the source DB.
                try (final PipedWriter pipeWriter = new PipedWriter(pipeReader);
                        final ResultSet rs = statSource.executeQuery("SCRIPT")) {
                    while (rs.next()) {
                        pipeWriter.write(rs.getString(1) + "\n");
                    }
                } catch (SQLException | IOException ex) {
                    throw new IllegalStateException("Producing script from the source DB is failing.", ex);
                }
            }
        });

        thread.shutdown();

        return threadFuture;
    }

}
