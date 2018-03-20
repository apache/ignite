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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientAuthenticationException;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientHandshakeException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionResetException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;
import org.apache.ignite.internal.visor.baseline.VisorBaselineOperation;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskArg;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskResult;
import org.apache.ignite.internal.visor.misc.*;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.visor.baseline.VisorBaselineOperation.ADD;
import static org.apache.ignite.internal.visor.baseline.VisorBaselineOperation.COLLECT;
import static org.apache.ignite.internal.visor.baseline.VisorBaselineOperation.REMOVE;
import static org.apache.ignite.internal.visor.baseline.VisorBaselineOperation.SET;
import static org.apache.ignite.internal.visor.baseline.VisorBaselineOperation.VERSION;

/**
 * Class that execute several commands passed via command line.
 */
public class CommandHandler {
    /** */
    static final String DFLT_HOST = "127.0.0.1";

    /** */
    static final String DFLT_PORT = "11211";

    /** */
    static final String CMD_HELP = "--help";

    /** */
    static final String CMD_HOST = "--host";

    /** */
    static final String CMD_PORT = "--port";

    /** */
    static final String CMD_PASSWORD = "--password";

    /** */
    static final String CMD_USER = "--user";

    /** */
    static final String CMD_NODES = "--nodes";

    /** */
    static final String BASELINE_ADD = "add";

    /** */
    static final String BASELINE_REMOVE = "remove";

    /** */
    static final String BASELINE_SET = "set";

    /** */
    static final String BASELINE_SET_VERSION = "version";

    /** */
    static final String WAL_PRINT = "print";

    /** */
    static final String WAL_DELETE = "delete";

    /** */
    static final String DELIM = "--------------------------------------------------------------------------------";

    /** */
    static final String CMD_ACTIVATE = "--activate";

    /** */
    static final String CMD_BASE_LINE = "--baseline";

    /** */
    static final String CMD_DEACTIVATE = "--deactivate";

    /** */
    static final String CMD_STATE = "--state";

    /** */
    static final String CMD_WAL = "--wal";

    /** */
    public static final int EXIT_CODE_OK = 0;

    /** */
    public static final int EXIT_CODE_INVALID_ARGUMENTS = 1;

    /** */
    public static final int EXIT_CODE_CONNECTION_FAILED = 2;

    /** */
    public static final int ERR_AUTHENTICATION_FAILED = 3;

    /** */
    public static final int EXIT_CODE_UNEXPECTED_ERROR = 4;

    /**
     * Output specified string to console.
     *
     * @param s String to output.
     */
    private void log(String s) {
        System.out.println(s);
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
     * @param s Optional message.
     * @param e Error to print.
     */
    private int error(int errCode, String s, Throwable e) {
        if (!F.isEmpty(s))
            log(s);

        String msg = e.getMessage();

        if (F.isEmpty(msg))
            msg = e.getClass().getName();

        if (msg.startsWith("Failed to handle request")) {
            int p = msg.indexOf("err=");

            msg = msg.substring(p + 4, msg.length() - 1);
        }

        log("Error: " + msg);

        return errCode;
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
     * Parses and validates arguments.
     *
     * @param rawArgs Array of arguments.
     * @return Arguments bean.
     * @throws IllegalArgumentException In case arguments aren't valid.
     */
    @NotNull Arguments parseAndValidate(String... rawArgs) {
        String host = DFLT_HOST;

        String port = DFLT_PORT;

        String user = null;

        String pwd = null;

        String baselineAct = "";

        String baselineArgs = "";

        String walAct = "";

        String walArgs = "";

        String nodes = "";

        List<String> commands = new ArrayList<>();

        Iterator<String> it = Arrays.asList(rawArgs).iterator();

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

                case CMD_USER:
                    user = nextArg(it, "Expected user name");
                    break;

                case CMD_PASSWORD:
                    pwd = nextArg(it, "Expected password");
                    break;

                case CMD_NODES:
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
                    break;

                case CMD_WAL:
                    commands.add(CMD_WAL);

                    if (it.hasNext()) {
                        walAct = it.next().toLowerCase();

                        if (WAL_PRINT.equals(walAct) || WAL_DELETE.equals(walAct))
                            walArgs = it.hasNext() ? nextArg(it,"Unexpected WAL arguments") : "";
                        else
                            throw new IllegalArgumentException("Unexpected argument for " + CMD_WAL + ": " + walAct);
                    }
                    else
                        throw new IllegalArgumentException("Expected arguments for " + CMD_WAL);

                    break;

            }
        }

        int sz = commands.size();

        if (sz < 1)
            throw new IllegalArgumentException("No action was specified");

        if (sz > 1)
            throw new IllegalArgumentException("Only one action can be specified, but found: " + sz);

        String cmd = commands.get(0);

        boolean hasUsr = F.isEmpty(user);
        boolean hasPwd = F.isEmpty(pwd);

        if (hasUsr != hasPwd)
            throw new IllegalArgumentException("Both user and password should be specified");

        return new Arguments(cmd, host, port, user, pwd, baselineAct, baselineArgs, walAct, walArgs);
    }

    /**
     * Activate cluster.
     *
     * @param client Client.
     * @throws GridClientException If failed to activate.
     */
    private void activate(GridClient client) throws Throwable {
        try {
            GridClientClusterState state = client.state();

            state.active(true);

            log("Cluster activated");
        }
        catch (Throwable e) {
            log("Failed to activate cluster.");

            throw e;
        }
    }

    /**
     * Deactivate cluster.
     *
     * @param client Client.
     * @throws Throwable If failed to deactivate.
     */
    private void deactivate(GridClient client) throws Throwable {
        try {
            GridClientClusterState state = client.state();

            state.active(false);

            log("Cluster deactivated");
        }
        catch (Throwable e) {
            log("Failed to deactivate cluster.");

            throw e;
        }
    }

    /**
     * Print cluster state.
     *
     * @param client Client.
     * @throws Throwable If failed to print state.
     */
    private void state(GridClient client) throws Throwable {
        try {
            GridClientClusterState state = client.state();

            log("Cluster is " + (state.active() ? "active" : "inactive"));
        }
        catch (Throwable e) {
            log("Failed to get cluster state.");

            throw e;
        }
    }

    /**
     *
     * @param client Client
     * @return Task result.
     * @throws GridClientException If failed to execute task.
     */
    private <R> R executeTask(GridClient client, Class<?> taskCls, Object taskArgs) throws GridClientException {
        GridClientCompute compute = client.compute();

        GridClientNode node = getBalancedNode(compute);

        return compute.projection(node).execute(taskCls.getName(),
            new VisorTaskArgument<>(node.nodeId(), taskArgs, false));
    }

    /**
     *  Get balanced node.
     *
     *  @param compute Compute projection
     *  @return Node.
     *  @throws GridClientException If failed to pick node.
     */
    private GridClientNode getBalancedNode(GridClientCompute compute) throws GridClientException {
        List<GridClientNode> connectableNodes = new ArrayList<>();

        for (GridClientNode node : compute.nodes())
            if (node.connectable())
                connectableNodes.add(node);

        if (F.isEmpty(connectableNodes))
            throw new GridClientDisconnectedException("Connectable node not found", null);

        return compute.balancer().balancedNode(connectableNodes);
    }

    /**
     * Change baseline.
     *
     * @param client Client.
     * @param baselineAct Baseline action to execute.  @throws GridClientException If failed to execute baseline action.
     * @param baselineArgs Baseline action arguments.
     * @throws Throwable If failed to execute baseline action.
     */
    private void baseline(GridClient client, String baselineAct, String baselineArgs) throws Throwable {
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
     * Execute WAL command.
     *
     * @param client Client.
     * @param walAct Wal action to execute.
     * @param walArgs Wal args.
     * @throws Throwable If failed to execute wal action.
     */
    private void wal(GridClient client, String walAct, String walArgs) throws Throwable {
        switch (walAct){
            case WAL_DELETE:
                deleteUnusedWalSegments(client, walArgs);
                break;
            case WAL_PRINT:
            default:
                printUnusedWalSegments(client, walArgs);
                break;
        }

    }

    /**
     * Execute delete unused WAL segments task.
     *
     * @param client Client.
     * @param walArgs Wal args.
     */
    private void deleteUnusedWalSegments(GridClient client, String walArgs) throws Throwable {
        VisorWalTaskResult res = executeTask(client, VisorWalTask.class,
                walArg(VisorWalTaskOperation.DELETE_UNUSED_WAL_SEGMENTS, walArgs));
        printDeleteWalSegments0(res);
    }

    /**
     * Execute print unused WAL segments task.
     *
     * @param client Client.
     * @param walArgs Wal args.
     */
    private void printUnusedWalSegments(GridClient client, String walArgs) throws Throwable {
        VisorWalTaskResult res = executeTask(client, VisorWalTask.class,
                walArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS, walArgs));
        printUnusedWalSegments0(res);
    }

    /**
     * Print list of unused wal segments.
     *
     * @param res Task result with baseline topology.
     */
    private void printUnusedWalSegments0(VisorWalTaskResult res) {
        log("Unused wal segments per node:");
        nl();

        Map<String, Collection<String>> okRes = res.results();
        Map<String, Exception> failRes = res.exceptions();
        Map<String, VisorClusterNode> nodesInfo = res.getNodesInfo();

        for(Map.Entry<String, Collection<String>> entry: okRes.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            log("Node=" + node.getConsistentId());
            log("     addresses " + U.addressesAsString(node.getAddresses(),node.getHostnames()));

            for(String fileName: entry.getValue())
                log("   " + fileName);
            nl();
        }

        for(Map.Entry<String, Exception> entry: failRes.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            log("Node=" + node.getConsistentId());
            log("     addresses " + U.addressesAsString(node.getAddresses(),node.getHostnames()));

            log("   failed with error: " + entry.getValue().getMessage());
            nl();
        }
    }

    /**
     * Print list of unused wal segments.
     *
     * @param res Task result with baseline topology.
     */
    private void printDeleteWalSegments0(VisorWalTaskResult res) {
        log("WAL segments deleted for nodes:");
        nl();

        Map<String, Collection<String>> okRes = res.results();
        Map<String, Exception> failRes = res.exceptions();
        Map<String, VisorClusterNode> nodesInfo = res.getNodesInfo();

        for(Map.Entry<String, Collection<String>> entry: okRes.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            log("Node=" + node.getConsistentId());
            log("     addresses " + U.addressesAsString(node.getAddresses(),node.getHostnames()));
            nl();
        }

        for(Map.Entry<String, Exception> entry: failRes.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            log("Node=" + node.getConsistentId());
            log("     addresses " + U.addressesAsString(node.getAddresses(),node.getHostnames()));

            log("   failed with error: " + entry.getValue().getMessage());
            nl();
        }
    }

    /**
     * Prepare baseline task argument.
     *
     * @param op Operation.
     * @param s Argument from command line.
     * @return Task argument.
     */
    private VisorBaselineTaskArg baselineArg(VisorBaselineOperation op, String s) {
        switch (op) {
            case ADD:
            case REMOVE:
            case SET:
                if(F.isEmpty(s))
                    throw new IllegalArgumentException("Empty list of consistent IDs");

                List<String> consistentIds = new ArrayList<>();

                for (String consistentId : s.split(","))
                    consistentIds.add(consistentId.trim());

                return new VisorBaselineTaskArg(op, -1, consistentIds);

            case VERSION:
                try {
                    long topVer = Long.parseLong(s);

                    return new VisorBaselineTaskArg(op, topVer, null);
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid topology version: " + s, e);
                }

            default:
                return new VisorBaselineTaskArg(op, -1, null);
        }
    }

    /**
     * Prepare WAL task argument.
     *
     * @param op Operation.
     * @param s Argument from command line.
     * @return Task argument.
     */
    private VisorWalTaskArg walArg(VisorWalTaskOperation op, String s){
        List<String> consistentIds = null;

        if (!F.isEmpty(s)) {
            consistentIds = new ArrayList<>();
            for (String consistentId : s.split(","))
                consistentIds.add(consistentId.trim());
        }

        switch (op){
            case DELETE_UNUSED_WAL_SEGMENTS:
            case PRINT_UNUSED_WAL_SEGMENTS:
                return new VisorWalTaskArg(op, consistentIds);
            default:
                return new VisorWalTaskArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS, consistentIds);
        }

    }

    /**
     * Print baseline topology.
     *
     * @param res Task result with baseline topology.
     */
    private void baselinePrint0(VisorBaselineTaskResult res) {
        log("Cluster state: " + (res.isActive() ? "active" : "inactive"));
        log("Current topology version: " + res.getTopologyVersion());
        nl();

        Map<String, VisorBaselineNode> baseline = res.getBaseline();
        Map<String, VisorBaselineNode> servers = res.getServers();

        if (F.isEmpty(baseline))
            log("Baseline nodes not found.");
        else {
            log("Baseline nodes:");

            for(VisorBaselineNode node : baseline.values()) {
                log("    ConsistentID=" + node.getConsistentId() + ", STATE=" +
                    (servers.containsKey(node.getConsistentId()) ? "ONLINE" : "OFFLINE"));
            }

            log(DELIM);
            log("Number of baseline nodes: " + baseline.size());

            nl();

            List<VisorBaselineNode> others = new ArrayList<>();

            for (VisorBaselineNode node : servers.values()) {
                if (!baseline.containsKey(node.getConsistentId()))
                    others.add(node);
            }

            if (F.isEmpty(others))
                log("Other nodes not found.");
            else {
                log("Other nodes:");

                for(VisorBaselineNode node : others)
                    log("    ConsistentID=" + node.getConsistentId());

                log("Number of other nodes: " + others.size());
            }
        }
    }

    /**
     * Print current baseline.
     *
     * @param client Client.
     */
    private void baselinePrint(GridClient client) throws GridClientException {
        VisorBaselineTaskResult res = executeTask(client, VisorBaselineTask.class, baselineArg(COLLECT, ""));

        baselinePrint0(res);
    }

    /**
     * Add nodes to baseline.
     *
     * @param client Client.
     * @param baselineArgs Baseline action arguments.
     * @throws Throwable If failed to add nodes to baseline.
     */
    private void baselineAdd(GridClient client, String baselineArgs) throws Throwable {
        try {
            VisorBaselineTaskResult res = executeTask(client, VisorBaselineTask.class, baselineArg(ADD, baselineArgs));

            baselinePrint0(res);
        }
        catch (Throwable e) {
            log("Failed to add nodes to baseline.");

            throw e;
        }
    }

    /**
     * Remove nodes from baseline.
     *
     * @param client Client.
     * @param consistentIds Consistent IDs.
     * @throws Throwable If failed to remove nodes from baseline.
     */
    private void baselineRemove(GridClient client, String consistentIds) throws Throwable {
        try {
            VisorBaselineTaskResult res = executeTask(client, VisorBaselineTask.class, baselineArg(REMOVE, consistentIds));

            baselinePrint0(res);
        }
        catch (Throwable e) {
            log("Failed to remove nodes from baseline.");

            throw e;
        }
    }

    /**
     * Set baseline.
     *
     * @param client Client.
     * @param consistentIds Consistent IDs.
     * @throws Throwable If failed to set baseline.
     */
    private void baselineSet(GridClient client, String consistentIds) throws Throwable {
        try {
            VisorBaselineTaskResult res = executeTask(client, VisorBaselineTask.class, baselineArg(SET, consistentIds));

            baselinePrint0(res);
        }
        catch (Throwable e) {
            log("Failed to set baseline.");

            throw e;
        }
    }

    /**
     * Set baseline by topology version.
     *
     * @param client Client.
     * @param arg Argument from command line.
     */
    private void baselineVersion(GridClient client, String arg) throws GridClientException {
        try {
            VisorBaselineTaskResult res = executeTask(client, VisorBaselineTask.class, baselineArg(VERSION, arg));

            baselinePrint0(res);
        }
        catch (Throwable e) {
            log("Failed to set baseline with specified topology version.");

            throw e;
        }
    }

    /**
     * @param e Exception to check.
     * @return {@code true} if specified exception is {@link GridClientAuthenticationException}.
     */
    private boolean isAuthError(Throwable e) {
        return X.hasCause(e, GridClientAuthenticationException.class);
    }

    /**
     * @param e Exception to check.
     * @return {@code true} if specified exception is a connection error.
     */
    private boolean isConnectionError(Throwable e) {
        return e instanceof GridClientClosedException ||
            e instanceof GridClientConnectionResetException ||
            e instanceof GridClientDisconnectedException ||
            e instanceof GridClientHandshakeException ||
            e instanceof GridServerUnreachableException;
    }

    /**
     * Print command usage.
     *
     * @param desc Command description.
     * @param cmd Command.
     */
    private void usage(String desc, String cmd) {
        log(desc);
        log("    control.sh [--host HOST_OR_IP] [--port PORT] [--user USER] [--password PASSWORD] " + cmd);
        nl();
    }

    /**
     * Parse and execute command.
     *
     * @param rawArgs Arguments to parse and execute.
     * @return Exit code.
     */
    public int execute(String... rawArgs) {
        log("Control utility [ver. " + ACK_VER_STR + "]");
        log(COPYRIGHT);
        log("User: " + System.getProperty("user.name"));
        log(DELIM);

        try {
            if (F.isEmpty(rawArgs) || (rawArgs.length == 1 && CMD_HELP.equalsIgnoreCase(rawArgs[0]))) {
                log("This utility can do the following commands:");

                usage("  Activate cluster:", CMD_ACTIVATE);
                usage("  Deactivate cluster:", CMD_DEACTIVATE);
                usage("  Print current cluster state:", CMD_STATE);
                usage("  Print cluster baseline topology:", CMD_BASE_LINE);
                usage("  Add nodes into baseline topology:", CMD_BASE_LINE + " add consistentId1[,consistentId2,....,consistentIdN]");
                usage("  Remove nodes from baseline topology:", CMD_BASE_LINE + " remove consistentId1[,consistentId2,....,consistentIdN]");
                usage("  Set baseline topology:", CMD_BASE_LINE + " set consistentId1[,consistentId2,....,consistentIdN]");
                usage("  Set baseline topology based on version:", CMD_BASE_LINE + " version topologyVersion");
                usage("  Print absolute path of unused archived wal segments on each node:", CMD_WAL);

                log("Default values:");
                log("    HOST_OR_IP=" + DFLT_HOST);
                log("    PORT=" + DFLT_PORT);
                nl();

                log("Exit codes:");
                log("    " + EXIT_CODE_OK + " - successful execution.");
                log("    " + EXIT_CODE_INVALID_ARGUMENTS + " - invalid arguments.");
                log("    " + EXIT_CODE_CONNECTION_FAILED + " - connection failed.");
                log("    " + ERR_AUTHENTICATION_FAILED + " - authentication failed.");
                log("    " + EXIT_CODE_UNEXPECTED_ERROR + " - unexpected error.");

                return EXIT_CODE_OK;
            }

            Arguments args = parseAndValidate(rawArgs);

            GridClientConfiguration cfg = new GridClientConfiguration();

            cfg.setServers(Collections.singletonList(args.host() + ":" + args.port()));

            if (!F.isEmpty(args.user())) {
                cfg.setSecurityCredentialsProvider(
                    new SecurityCredentialsBasicProvider(new SecurityCredentials(args.user(), args.password())));
            }

            try (GridClient client = GridClientFactory.start(cfg)) {

                switch (args.command()) {
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
                        baseline(client, args.baselineAction(), args.baselineArguments());
                        break;

                    case CMD_WAL:
                        wal(client, args.walAction(), args.walArguments());
                        break;
                }
            }

            return 0;
        }
        catch (IllegalArgumentException e) {
            return error(EXIT_CODE_INVALID_ARGUMENTS, "Check arguments.", e);
        }
        catch (Throwable e) {
            if (isAuthError(e))
                return error(ERR_AUTHENTICATION_FAILED, "Authentication error.", e);

            if (isConnectionError(e))
                return error(EXIT_CODE_CONNECTION_FAILED, "Connection to cluster failed.", e);

            return error(EXIT_CODE_UNEXPECTED_ERROR, "", e);
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

