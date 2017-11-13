/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.baseline.VisorBaselineAddTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineCollectorTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineCollectorTaskResult;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;
import org.apache.ignite.internal.visor.baseline.VisorBaselineRemoveTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineSetTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineVersionTask;

import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;

/**
 * Class that execute several commands passed via command line.
 */
public class CommandHandler {
    /** */
    private static final String DFLT_HOST = "127.0.0.1";

    /** */
    private static final String DFLT_PORT = "11211";

    /** */
    private static final String CMD_HELP = "--help";

    /** */
    private static final String CMD_HOST = "--host";

    /** */
    private static final String CMD_PORT = "--port";

    /** */
    private static final String CMD_ACTIVATE = "--activate";

    /** */
    private static final String CMD_DEACTIVATE = "--deactivate";

    /** */
    private static final String CMD_STATE = "--state";

    /** */
    private static final String CMD_BASE_LINE = "--baseline";

    /** */
    private static final String BASELINE_ADD = "add";

    /** */
    private static final String BASELINE_REMOVE = "remove";

    /** */
    private static final String BASELINE_SET = "set";

    /** */
    private static final String BASELINE_SET_VERSION = "version";

    /** */
    private static final String DELIM = "--------------------------------------------------------------------------------";

    /** */
    private static final int EXIT_CODE_OK = 0;

    /** */
    private static final int EXIT_CODE_INVALID_ARGUMENTS = 1;

    /** */
    private static final int EXIT_CODE_CONNECTION_FAILED = 2;

    /** */
    private static final int EXIT_CODE_UNEXPECTED_ERROR = 3;

    /**
     * Output specified string to console.
     *
     * @param s String to output.
     */
    private void log(String s) {
        System.out.println(s);
    }

    /**
     * Output some data to console.
     *
     * @param fmt Format string.
     * @param args Arguments.
     */
    private void logFmt(String fmt, Object... args) {
        System.out.printf(fmt, args);
    }

    /**
     * Output empty line.
     */
    private void nl() {
        System.out.println("");
    }

    /**
     * Print error to console.
     *
     * @param errCode Error code to return.
     * @param e Error to print.
     */
    private int error(int errCode, Throwable e) {
        String msg = e.getMessage();

        if (F.isEmpty(msg))
            msg = e.getClass().getName();

        log("Error: " + msg);

        return errCode;
    }

    /**
     * Print command usage.
     *
     * @param desc Command description.
     * @param cmd Command.
     */
    private void usage(String desc, String cmd) {
        log(desc);
        log("    control.sh [--host HOST_OR_IP] [--port PORT] " + cmd);
        nl();
    }

    /**
     * Extract next argument.
     *
     * @param it Arguments iterator.
     * @param err Error message.
     * @return Next argument value.
     */
    private String nextArg(Iterator<String> it, String err) {
        if (it.hasNext()) {
            String arg = it.next();

            if (arg.startsWith("--"))
                throw new IllegalArgumentException("Unexpected argument: " + arg);

            return arg;
        }

        throw new IllegalArgumentException(err);
    }

    /**
     * Activate cluster.
     *
     * @param client Client.
     * @throws GridClientException If failed to activate.
     */
    private void activate(GridClient client) throws GridClientException {
        GridClientClusterState state = client.state();

        state.active(true);
    }

    /**
     * Deactivate cluster.
     *
     * @param client Client.
     * @throws GridClientException If failed to deactivate.
     */
    private void deactivate(GridClient client) throws GridClientException {
        GridClientClusterState state = client.state();

        state.active(false);
    }

    /**
     * Print cluster state.
     *
     * @param client Client.
     * @throws GridClientException If failed to print state.
     */
    private void state(GridClient client) throws GridClientException {
        GridClientClusterState state = client.state();

        log("Cluster is " + (state.active() ? "active" : "inactive"));
    }

    /**
     *
     * @param client Client
     * @return Task result.
     * @throws GridClientException If failed to execute task.
     */
    private <R> R executeTask(GridClient client, Class<?> taskCls, Object taskArgs) throws GridClientException {
        GridClientCompute compute = client.compute();

        List<GridClientNode> nodes = new ArrayList<>();

        for (GridClientNode node : compute.nodes())
            if (node.connectable())
                nodes.add(node);

        if (F.isEmpty(nodes))
            throw new GridClientDisconnectedException("Connectable node not found", null);

        GridClientNode node = compute.balancer().balancedNode(nodes);

        return compute.projection(node).execute(taskCls.getName(),
            new VisorTaskArgument<>(node.nodeId(), taskArgs, false));
    }

    /**
     * Change baseline.
     *
     * @param client Client.
     * @param baselineAct Baseline action to execute.  @throws GridClientException If failed to execute baseline action.
     * @param baselineArgs Baseline action arguments.
     */
    private void baseline(GridClient client, String baselineAct, String baselineArgs) throws GridClientException {
        switch (baselineAct) {
            case BASELINE_ADD:
                baselineAdd(client, baselineArgs);
                break;

            case BASELINE_REMOVE:
                baselineRemove(client, baselineArgs);
                break;

            case BASELINE_SET:
                baselineSet(client, baselineArgs);
                break;

            case BASELINE_SET_VERSION:
                baselineVersion(client, baselineArgs);
                break;

            default:
                baselinePrint(client);
        }
    }

    /**
     * @param client Client.
     */
    private void baselinePrint(GridClient client) throws GridClientException {
        VisorBaselineCollectorTaskResult res = executeTask(client, VisorBaselineCollectorTask.class, null);

        log("Current topology version: " + res.getTopologyVersion());
        nl();

        List<VisorBaselineNode> baseline = res.getBaseline();

        if (F.isEmpty(baseline))
            log("Base line baseline not found!");
        else {
            log("Baseline nodes:");

            for(VisorBaselineNode node : baseline)
                logFmt("    ConsistentID=%s, STATE=%s%n", node.consistentId(), node.isAlive() ? "ONLINE" : "OFFLINE");

            log(DELIM);
            log("Number of baseline nodes: " + baseline.size());

            nl();

            List<VisorBaselineNode> others = res.getOthers();

            if (F.isEmpty(others))
                log("Other nodes not found!");
            else {
                log("Other nodes:");

                for(VisorBaselineNode node : others)
                    log("    ConsistentID=" + node.consistentId() /* TODO WC-251 add IP? */);

                log("Number of other nodes: " + others.size());
            }
        }
    }

    /**
     * @param client Client.
     * @param baselineArgs Baseline action arguments.
     */
    private void baselineAdd(GridClient client, String baselineArgs) throws GridClientException {
        executeTask(client, VisorBaselineAddTask.class, baselineArgs);
    }

    /**
     * @param client Client.
     * @param baselineArgs Baseline action arguments.
     */
    private void baselineRemove(GridClient client, String baselineArgs) throws GridClientException {
        executeTask(client, VisorBaselineRemoveTask.class, baselineArgs);
    }

    /**
     * @param client Client.
     * @param baselineArgs Baseline action arguments.
     */
    private void baselineSet(GridClient client, String baselineArgs) throws GridClientException {
        executeTask(client, VisorBaselineSetTask.class, baselineArgs);
    }

    /**
     * @param client Client.
     * @param baselineArgs Baseline action arguments.
     */
    private void baselineVersion(GridClient client, String baselineArgs) throws GridClientException {
        executeTask(client, VisorBaselineVersionTask.class, baselineArgs);
    }

    /**
     * Parse and execute command.
     *
     * @param args Arguments to parse and execute.
     * @return Exit code.
     */
    public int execute(String... args) {
        log("Control utility [ver. " + ACK_VER_STR + "]");
        log(COPYRIGHT);
        log("User: " + System.getProperty("user.name"));
        log(DELIM);

        try {
            if (F.isEmpty(args) || (args.length == 1 && CMD_HELP.equalsIgnoreCase(args[0]))){
                log("This utility can do the following commands:");

                usage("  Activate cluster:", CMD_ACTIVATE);
                usage("  Deactivate cluster:", CMD_DEACTIVATE);
                usage("  Print current cluster state:", CMD_STATE);
                usage("  Print cluster baseline topology:", CMD_BASE_LINE);
                usage("  Add nodes into baseline topology:", CMD_BASE_LINE + " add consistentId1[,consistentId2,....,consistentIdN]");
                usage("  Remove nodes from baseline topology:", CMD_BASE_LINE + " remove consistentId1[,consistentId2,....,consistentIdN]");
                usage("  Set baseline topology:", CMD_BASE_LINE + " set consistentId1[,consistentId2,....,consistentIdN]");
                usage("  Set baseline topology based on version:", CMD_BASE_LINE + " version topologyVersion");

                log("Default values:");
                log("    HOST_OR_IP=" + DFLT_HOST);
                log("    PORT=" + DFLT_PORT);
                nl();

                log("Exit codes:");
                log("    " + EXIT_CODE_OK + " - successful execution.");
                log("    " + EXIT_CODE_INVALID_ARGUMENTS + " - invalid arguments.");
                log("    " + EXIT_CODE_CONNECTION_FAILED + " - connection failed.");
                log("    " + EXIT_CODE_UNEXPECTED_ERROR + " - unexpected error.");

                return EXIT_CODE_OK;
            }

            String host = DFLT_HOST;

            String port = DFLT_PORT;

            String baselineAct = "";

            String baselineArgs = "";

            List<String> commands = new ArrayList<>();

            Iterator<String> it = Arrays.asList(args).iterator();

            while (it.hasNext()) {
                String str = it.next().toLowerCase();

                switch (str) {
                    case CMD_HOST:
                        host = nextArg(it, "Expected host name");
                        break;

                    case CMD_PORT:
                        port = nextArg(it, "Expected port number");

                        try {
                            int p = Integer.parseInt(port);

                            if (p <= 0 || p > 65535)
                                throw new IllegalArgumentException("Invalid value for port: " + port);
                        }
                        catch (NumberFormatException ignored) {
                            throw new IllegalArgumentException("Invalid value for port: " + port);
                        }
                        break;

                    case CMD_ACTIVATE:
                    case CMD_DEACTIVATE:
                    case CMD_STATE:
                        commands.add(str);
                        break;

                    case CMD_BASE_LINE:
                        commands.add(CMD_BASE_LINE);

                        if (it.hasNext()) {
                            baselineAct = it.next().toLowerCase();

                            if (BASELINE_ADD.equals(baselineAct) || BASELINE_REMOVE.equals(baselineAct) ||
                                BASELINE_SET.equals(baselineAct) || BASELINE_SET_VERSION.equals(baselineAct))
                                baselineArgs = nextArg(it, "Expected baseline arguments");
                            else
                                throw new IllegalArgumentException("Unexpected argument for " + CMD_BASE_LINE + ": "
                                        + baselineAct);
                        }

                }
            }

            int sz = commands.size();

            if (sz < 1)
                throw new IllegalArgumentException("No action was specified");

            if (sz > 1)
                throw new IllegalArgumentException("Only one action can be specified, but found: " + sz);

            GridClientConfiguration cfg = new GridClientConfiguration();

            cfg.setServers(Collections.singletonList(host + ":" + port));

            try (GridClient client = GridClientFactory.start(cfg)) {
                String cmd = commands.get(0);

                switch (cmd) {
                    case CMD_ACTIVATE:
                        activate(client);
                        break;

                    case CMD_DEACTIVATE:
                        deactivate(client);
                        break;

                    case CMD_STATE:
                        state(client);
                        break;

                    case CMD_BASE_LINE:
                        baseline(client, baselineAct, baselineArgs);
                        break;
                }
            }

            return 0;
        }
        catch (IllegalArgumentException e) {
            return error(EXIT_CODE_INVALID_ARGUMENTS, e);
        }
        catch (Throwable e) {
            return error(EXIT_CODE_UNEXPECTED_ERROR, e);
        }
    }

    /**
     * @param args Arguments to parse and apply.
     */
    public static void main(String[] args) {
        CommandHandler hnd = new CommandHandler();

        System.exit(hnd.execute(args));
    }
}
