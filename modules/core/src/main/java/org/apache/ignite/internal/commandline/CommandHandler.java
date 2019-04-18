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

import java.io.IOException;
import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteNodeAttributes;
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
import org.apache.ignite.internal.client.ssl.GridSslBasicContextFactory;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.baseline.AutoAdjustCommandArg;
import org.apache.ignite.internal.commandline.baseline.BaselineArguments;
import org.apache.ignite.internal.commandline.baseline.BaselineCommand;
import org.apache.ignite.internal.commandline.cache.CacheArguments;
import org.apache.ignite.internal.commandline.cache.CacheCommand;
import org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ListCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTask;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskResult;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTask;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskArg;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskResult;
import org.apache.ignite.internal.processors.cache.verify.CacheInfo;
import org.apache.ignite.internal.processors.cache.verify.ContentionInfo;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.baseline.VisorBaselineAutoAdjustSettings;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;
import org.apache.ignite.internal.visor.baseline.VisorBaselineOperation;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskArg;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskResult;
import org.apache.ignite.internal.visor.cache.VisorCacheAffinityConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheConfigurationCollectorTask;
import org.apache.ignite.internal.visor.cache.VisorCacheConfigurationCollectorTaskArg;
import org.apache.ignite.internal.visor.cache.VisorCacheEvictionConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheNearConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheRebalanceConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheStoreConfiguration;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceJobResult;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTask;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTaskArg;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTaskResult;
import org.apache.ignite.internal.visor.misc.VisorClusterNode;
import org.apache.ignite.internal.visor.misc.VisorWalTask;
import org.apache.ignite.internal.visor.misc.VisorWalTaskArg;
import org.apache.ignite.internal.visor.misc.VisorWalTaskOperation;
import org.apache.ignite.internal.visor.misc.VisorWalTaskResult;
import org.apache.ignite.internal.visor.query.VisorQueryConfiguration;
import org.apache.ignite.internal.visor.tx.FetchNearXidVersionTask;
import org.apache.ignite.internal.visor.tx.TxKeyLockType;
import org.apache.ignite.internal.visor.tx.TxMappingType;
import org.apache.ignite.internal.visor.tx.TxVerboseId;
import org.apache.ignite.internal.visor.tx.TxVerboseInfo;
import org.apache.ignite.internal.visor.tx.TxVerboseKey;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;
import org.apache.ignite.internal.visor.verify.IndexIntegrityCheckIssue;
import org.apache.ignite.internal.visor.verify.IndexValidationIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesPartitionResult;
import org.apache.ignite.internal.visor.verify.VisorContentionTask;
import org.apache.ignite.internal.visor.verify.VisorContentionTaskArg;
import org.apache.ignite.internal.visor.verify.VisorContentionTaskResult;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTask;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTask;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskResult;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskV2;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskArg;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;
import org.apache.ignite.internal.visor.verify.VisorViewCacheCmd;
import org.apache.ignite.internal.visor.verify.VisorViewCacheTask;
import org.apache.ignite.internal.visor.verify.VisorViewCacheTaskArg;
import org.apache.ignite.internal.visor.verify.VisorViewCacheTaskResult;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityCredentialsProvider;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.transactions.TransactionState;

import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.commandline.Command.ACTIVATE;
import static org.apache.ignite.internal.commandline.Command.BASELINE;
import static org.apache.ignite.internal.commandline.Command.CACHE;
import static org.apache.ignite.internal.commandline.Command.DEACTIVATE;
import static org.apache.ignite.internal.commandline.Command.STATE;
import static org.apache.ignite.internal.commandline.Command.TX;
import static org.apache.ignite.internal.commandline.Command.WAL;
import static org.apache.ignite.internal.commandline.OutputFormat.MULTI_LINE;
import static org.apache.ignite.internal.commandline.OutputFormat.SINGLE_LINE;
import static org.apache.ignite.internal.commandline.baseline.BaselineCommand.of;
import static org.apache.ignite.internal.commandline.cache.CacheCommand.CONTENTION;
import static org.apache.ignite.internal.commandline.cache.CacheCommand.DISTRIBUTION;
import static org.apache.ignite.internal.commandline.cache.CacheCommand.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheCommand.HELP;
import static org.apache.ignite.internal.commandline.cache.CacheCommand.IDLE_VERIFY;
import static org.apache.ignite.internal.commandline.cache.CacheCommand.LIST;
import static org.apache.ignite.internal.commandline.cache.CacheCommand.RESET_LOST_PARTITIONS;
import static org.apache.ignite.internal.commandline.cache.CacheCommand.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg.USER_ATTRIBUTES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CACHE_FILTER;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CHECK_CRC;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.DUMP;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.EXCLUDE_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.SKIP_ZEROS;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.CONFIG;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.GROUP;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.OUTPUT_FORMAT;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.SEQUENCE;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.CACHES;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.GROUPS;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.SEQ;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;

/**
 * Class that execute several commands passed via command line.
 */
public class CommandHandler {
    /** */
    static final String DFLT_HOST = "127.0.0.1";

    /** */
    static final String DFLT_PORT = "11211";

    /** */
    private static final String CMD_HELP = "--help";

    /** */
    private static final String CMD_HOST = "--host";

    /** */
    private static final String CMD_PORT = "--port";

    /** */
    private static final String CMD_PASSWORD = "--password";

    /** */
    private static final String CMD_USER = "--user";

    /** Option is used for auto confirmation. */
    private static final String CMD_AUTO_CONFIRMATION = "--yes";

    /** */
    private static final String CMD_PING_INTERVAL = "--ping-interval";

    /** */
    private static final String CMD_PING_TIMEOUT = "--ping-timeout";

    /** One cache filter option should used message. */
    public static final String ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG = "Should use only one of option: " +
        EXCLUDE_CACHES + ", " + CACHE_FILTER + " or pass caches explicitly";

    // SSL configuration section

    /** */
    private static final String CMD_SSL_PROTOCOL = "--ssl-protocol";

    /** */
    private static final String CMD_SSL_KEY_ALGORITHM = "--ssl-key-algorithm";

    /** */
    private static final String CMD_SSL_CIPHER_SUITES = "--ssl-cipher-suites";

    /** */
    private static final String CMD_KEYSTORE = "--keystore";

    /** */
    private static final String CMD_KEYSTORE_PASSWORD = "--keystore-password";

    /** */
    private static final String CMD_KEYSTORE_TYPE = "--keystore-type";

    /** */
    private static final String CMD_TRUSTSTORE = "--truststore";

    /** */
    private static final String CMD_TRUSTSTORE_PASSWORD = "--truststore-password";

    /** */
    private static final String CMD_TRUSTSTORE_TYPE = "--truststore-type";

    /** List of optional auxiliary commands. */
    private static final Set<String> AUX_COMMANDS = new HashSet<>();

    static {
        AUX_COMMANDS.add(CMD_HELP);

        AUX_COMMANDS.add(CMD_HOST);
        AUX_COMMANDS.add(CMD_PORT);

        AUX_COMMANDS.add(CMD_PASSWORD);
        AUX_COMMANDS.add(CMD_USER);

        AUX_COMMANDS.add(CMD_AUTO_CONFIRMATION);

        AUX_COMMANDS.add(CMD_PING_INTERVAL);
        AUX_COMMANDS.add(CMD_PING_TIMEOUT);

        AUX_COMMANDS.add(CMD_SSL_PROTOCOL);
        AUX_COMMANDS.add(CMD_SSL_KEY_ALGORITHM);
        AUX_COMMANDS.add(CMD_SSL_CIPHER_SUITES);

        AUX_COMMANDS.add(CMD_KEYSTORE);
        AUX_COMMANDS.add(CMD_KEYSTORE_PASSWORD);
        AUX_COMMANDS.add(CMD_KEYSTORE_TYPE);

        AUX_COMMANDS.add(CMD_TRUSTSTORE);
        AUX_COMMANDS.add(CMD_TRUSTSTORE_PASSWORD);
        AUX_COMMANDS.add(CMD_TRUSTSTORE_TYPE);
    }

    /** Broadcast uuid. */
    private static final UUID BROADCAST_UUID = UUID.randomUUID();

    /** */
    public static final String CONFIRM_MSG = "y";

    /** */
    static final String WAL_PRINT = "print";

    /** */
    static final String WAL_DELETE = "delete";

    /** */
    static final String DELIM = "--------------------------------------------------------------------------------";

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

    /** */
    private static final long DFLT_PING_INTERVAL = 5000L;

    /** */
    private static final long DFLT_PING_TIMEOUT = 30_000L;

    /** */
    private static final Scanner IN = new Scanner(System.in);

    /** Validate indexes task name. */
    private static final String VALIDATE_INDEXES_TASK = "org.apache.ignite.internal.visor.verify.VisorValidateIndexesTask";

    /** */
    private static final String TX_LIMIT = "--limit";

    /** */
    private static final String TX_ORDER = "--order";

    /** */
    private static final String TX_SERVERS = "--servers";

    /** */
    private static final String TX_CLIENTS = "--clients";

    /** */
    private static final String TX_DURATION = "--min-duration";

    /** */
    private static final String TX_SIZE = "--min-size";

    /** */
    private static final String TX_LABEL = "--label";

    /** */
    private static final String TX_NODES = "--nodes";

    /** */
    private static final String TX_XID = "--xid";

    /** */
    private static final String TX_KILL = "--kill";

    /** */
    private static final String TX_INFO = "--info";

    /** Utility name. */
    private static final String UTILITY_NAME = "control.sh";

    /** Common options. */
    private static final String COMMON_OPTIONS = j(" ", getCommonOptions());

    /** Utility name with common options. */
    private static final String UTILITY_NAME_WITH_COMMON_OPTIONS = j(" ", UTILITY_NAME, COMMON_OPTIONS);

    /** Indent for help output. */
    private static final String INDENT = "  ";

    /** Double indent. */
    private static final String DOUBLE_INDENT = INDENT + INDENT;

    /** */
    private static final String NULL = "null";

    /** */
    private static final String NODE_ID = "nodeId";

    /** */
    private static final String OP_NODE_ID = op(NODE_ID);

    /** */
    private Iterator<String> argsIt;

    /** */
    private String peekedArg;

    /** */
    private Object lastOperationRes;

    /** */
    private GridClientConfiguration clientCfg;

    /** Check if experimental commands are enabled. Default {@code false}. */
    private final boolean enableExperimental = IgniteSystemProperties.getBoolean(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, false);

    /** Console instance. Public access needs for tests. */
    public GridConsole console = GridConsoleAdapter.getInstance();

    /**
     * Creates list of common utility options.
     *
     * @return Array of common utility options.
     */
    private static String[] getCommonOptions() {
        List<String> list = new ArrayList<>(32);

        list.add(op(CMD_HOST, "HOST_OR_IP"));
        list.add(op(CMD_PORT, "PORT"));
        list.add(op(CMD_USER, "USER"));
        list.add(op(CMD_PASSWORD, "PASSWORD"));
        list.add(op(CMD_PING_INTERVAL, "PING_INTERVAL"));
        list.add(op(CMD_PING_TIMEOUT, "PING_TIMEOUT"));

        list.add(op(CMD_SSL_PROTOCOL, "SSL_PROTOCOL[, SSL_PROTOCOL_2, ..., SSL_PROTOCOL_N]"));
        list.add(op(CMD_SSL_CIPHER_SUITES, "SSL_CIPHER_1[, SSL_CIPHER_2, ..., SSL_CIPHER_N]"));
        list.add(op(CMD_SSL_KEY_ALGORITHM, "SSL_KEY_ALGORITHM"));
        list.add(op(CMD_KEYSTORE_TYPE, "KEYSTORE_TYPE"));
        list.add(op(CMD_KEYSTORE, "KEYSTORE_PATH"));
        list.add(op(CMD_KEYSTORE_PASSWORD, "KEYSTORE_PASSWORD"));
        list.add(op(CMD_TRUSTSTORE_TYPE, "TRUSTSTORE_TYPE"));
        list.add(op(CMD_TRUSTSTORE, "TRUSTSTORE_PATH"));
        list.add(op(CMD_TRUSTSTORE_PASSWORD, "TRUSTSTORE_PASSWORD"));

        return list.toArray(new String[0]);
    }

    /**
     * Output specified string to console.
     *
     * @param s String to output.
     */
    private void log(String s) {
        System.out.println(s);
    }

    /**
     * Adds indent to begin of object's string representation.
     *
     * @param o Input object.
     * @return Indented string.
     */
    private static String i(Object o) {
        return i(o, 1);
    }

    /**
     * Adds specified indents to begin of object's string representation.
     *
     * @param o Input object.
     * @param indentCnt Number of indents.
     * @return Indented string.
     */
    private static String i(Object o, int indentCnt) {
        assert indentCnt >= 0;

        String s = o == null ? null : o.toString();

        switch (indentCnt) {
            case 0:
                return s;

            case 1:
                return INDENT + s;

            default:
                int sLen = s == null ? 4 : s.length();

                SB sb = new SB(sLen + indentCnt * INDENT.length());

                for (int i = 0; i < indentCnt; i++)
                    sb.a(INDENT);

                return sb.a(s).toString();
        }
    }

    /**
     * Format and output specified string to console.
     *
     * @param format A format string as described in Format string syntax.
     * @param args Arguments referenced by the format specifiers in the format string.
     */
    private void log(String format, Object... args) {
        System.out.printf(format, args);
    }

    /**
     * Provides a prompt, then reads a single line of text from the console.
     *
     * @param prompt text
     * @return A string containing the line read from the console
     */
    private String readLine(String prompt) {
        System.out.print(prompt);

        return IN.nextLine();
    }

    /**
     * Output empty line.
     */
    private void nl() {
        System.out.println();
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
     * Requests interactive user confirmation if forthcoming operation is dangerous.
     *
     * @param args Arguments.
     * @return {@code true} if operation confirmed (or not needed), {@code false} otherwise.
     */
    private boolean confirm(Arguments args) {
        String prompt = confirmationPrompt(args);

        if (prompt == null)
            return true;

        return CONFIRM_MSG.equalsIgnoreCase(readLine(prompt));
    }

    /**
     * @param args Arguments.
     * @return Prompt text if confirmation needed, otherwise {@code null}.
     */
    private String confirmationPrompt(Arguments args) {
        String str = null;

        switch (args.command()) {
            case DEACTIVATE:
                str = "Warning: the command will deactivate a cluster.";

                break;

            case BASELINE:
                if (BaselineCommand.COLLECT != args.baselineArguments().getCmd())
                    str = "Warning: the command will perform changes in baseline.";

                break;

            case WAL:
                if (WAL_DELETE.equals(args.walAction()))
                    str = "Warning: the command will delete unused WAL segments.";

                break;

            case TX:
                if (args.transactionArguments().getOperation() == VisorTxOperation.KILL)
                    str = "Warning: the command will kill some transactions.";

                break;

            default:
                break;
        }

        return str == null ? null : str + "\nPress '" + CONFIRM_MSG + "' to continue . . . ";
    }

    /**
     * @param rawArgs Arguments.
     */
    private void initArgIterator(List<String> rawArgs) {
        argsIt = rawArgs.iterator();
        peekedArg = null;
    }

    /**
     * @return Returns {@code true} if the iteration has more elements.
     */
    private boolean hasNextArg() {
        return peekedArg != null || argsIt.hasNext();
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
     * @param client Client.
     * @param taskCls Task class.
     * @param taskArgs Task arguments.
     * @return Task result.
     * @throws GridClientException If failed to execute task.
     */
    private <R> R executeTask(GridClient client, Class<? extends ComputeTask<?, R>> taskCls,
        Object taskArgs) throws GridClientException {
        return executeTaskByNameOnNode(client, taskCls.getName(), taskArgs, null);
    }

    /**
     * @param client Client.
     * @return List of hosts.
     */
    private Stream<IgniteBiTuple<GridClientNode, String>> listHosts(GridClient client) throws GridClientException {
        return client.compute()
            .nodes(GridClientNode::connectable)
            .stream()
            .flatMap(node -> Stream.concat(
                node.tcpAddresses() == null ? Stream.empty() : node.tcpAddresses().stream(),
                node.tcpHostNames() == null ? Stream.empty() : node.tcpHostNames().stream()
            ).map(addr -> new IgniteBiTuple<>(node, addr + ":" + node.tcpPort())));
    }

    /**
     * @param client Client.
     * @return List of hosts.
     */
    private Stream<IgniteBiTuple<GridClientNode, List<String>>> listHostsByClientNode(
        GridClient client
    ) throws GridClientException {
        return client.compute().nodes(GridClientNode::connectable).stream()
            .map(
                node -> new IgniteBiTuple<>(
                    node,
                    Stream.concat(
                        node.tcpAddresses() == null ? Stream.empty() : node.tcpAddresses().stream(),
                        node.tcpHostNames() == null ? Stream.empty() : node.tcpHostNames().stream()
                    )
                        .map(addr -> addr + ":" + node.tcpPort()).collect(Collectors.toList())
                )
            );
    }

    /**
     * @param client Client
     * @param taskClsName Task class name.
     * @param taskArgs Task args.
     * @param nodeId Node ID to execute task at (if null, random node will be chosen by balancer).
     * @return Task result.
     * @throws GridClientException If failed to execute task.
     */
    private <R> R executeTaskByNameOnNode(
        GridClient client,
        String taskClsName,
        Object taskArgs,
        UUID nodeId
    ) throws GridClientException {
        GridClientCompute compute = client.compute();

        if (nodeId == BROADCAST_UUID) {
            Collection<GridClientNode> nodes = compute.nodes(GridClientNode::connectable);

            if (F.isEmpty(nodes))
                throw new GridClientDisconnectedException("Connectable nodes not found", null);

            List<UUID> nodeIds = nodes.stream()
                .map(GridClientNode::nodeId)
                .collect(Collectors.toList());

            return client.compute().execute(taskClsName, new VisorTaskArgument<>(nodeIds, taskArgs, false));
        }

        GridClientNode node = null;

        if (nodeId == null) {
            // Prefer node from connect string.
            final String cfgAddr = clientCfg.getServers().iterator().next();

            String[] parts = cfgAddr.split(":");

            if (DFLT_HOST.equals(parts[0])) {
                InetAddress addr;

                try {
                    addr = IgniteUtils.getLocalHost();
                }
                catch (IOException e) {
                    throw new GridClientException("Can't get localhost name.", e);
                }

                if (addr.isLoopbackAddress())
                    throw new GridClientException("Can't find localhost name.");

                String origAddr = addr.getHostName() + ":" + parts[1];

                node = listHosts(client).filter(tuple -> origAddr.equals(tuple.get2())).findFirst().map(IgniteBiTuple::get1).orElse(null);

                if (node == null)
                    node = listHostsByClientNode(client).filter(tuple -> tuple.get2().size() == 1 && cfgAddr.equals(tuple.get2().get(0))).
                        findFirst().map(IgniteBiTuple::get1).orElse(null);
            }
            else
                node = listHosts(client).filter(tuple -> cfgAddr.equals(tuple.get2())).findFirst().map(IgniteBiTuple::get1).orElse(null);

            // Otherwise choose random node.
            if (node == null)
                node = getBalancedNode(compute);
        }
        else {
            for (GridClientNode n : compute.nodes()) {
                if (n.connectable() && nodeId.equals(n.nodeId())) {
                    node = n;

                    break;
                }
            }

            if (node == null)
                throw new IllegalArgumentException("Node with id=" + nodeId + " not found");
        }

        return compute.projection(node).execute(taskClsName, new VisorTaskArgument<>(node.nodeId(), taskArgs, false));
    }

    /**
     * @param compute instance
     * @return balanced node
     */
    private GridClientNode getBalancedNode(GridClientCompute compute) throws GridClientException {
        Collection<GridClientNode> nodes = compute.nodes(GridClientNode::connectable);

        if (F.isEmpty(nodes))
            throw new GridClientDisconnectedException("Connectable node not found", null);

        return compute.balancer().balancedNode(nodes);
    }

    /**
     * Executes --cache subcommand.
     *
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private Object cache(GridClient client, CacheArguments cacheArgs) throws Throwable {
        switch (cacheArgs.command()) {
            case HELP:
                printCacheHelp();

                break;

            case IDLE_VERIFY:
                cacheIdleVerify(client, cacheArgs);

                break;

            case VALIDATE_INDEXES:
                cacheValidateIndexes(client, cacheArgs);

                break;

            case CONTENTION:
                cacheContention(client, cacheArgs);

                break;

            case DISTRIBUTION:
                cacheDistribution(client, cacheArgs);

                break;

            case RESET_LOST_PARTITIONS:
                cacheResetLostPartitions(client, cacheArgs);

                break;

            case FIND_AND_DELETE_GARBAGE:
                return findAndDeleteGarbage(client, cacheArgs);

            default:
                cacheView(client, cacheArgs);

                break;
        }

        return null;
    }

    /** */
    private void printCacheHelp() {
        log(i("The '" + CACHE + " subcommand' is used to get information about and perform actions with caches. The command has the following syntax:"));
        nl();
        log(i(UTILITY_NAME_WITH_COMMON_OPTIONS + " " + CACHE + " [subcommand] <subcommand_parameters>"));
        nl();
        log(i("The subcommands that take " + OP_NODE_ID + " as an argument ('" + LIST + "', '" + CONTENTION + "' and '" + VALIDATE_INDEXES + "') will be executed on the given node or on all server nodes if the option is not specified. Other commands will run on a random server node."));
        nl();
        nl();
        log(i("Subcommands:"));

        String CACHES = "cacheName1,...,cacheNameN";

        usageCache(LIST, "regexPattern", op(or(GROUP, SEQUENCE)), OP_NODE_ID, op(CONFIG), op(OUTPUT_FORMAT, MULTI_LINE));
        usageCache(CONTENTION, "minQueueSize", OP_NODE_ID, op("maxPrint"));
        usageCache(IDLE_VERIFY, op(DUMP), op(SKIP_ZEROS), op(CHECK_CRC),
            op(EXCLUDE_CACHES, CACHES), op(CACHE_FILTER, or(CacheFilterEnum.values())), op(CACHES));
        usageCache(VALIDATE_INDEXES, op(CACHES), OP_NODE_ID, op(or(CHECK_FIRST + " N", CHECK_THROUGH + " K")));
        usageCache(DISTRIBUTION, or(NODE_ID, NULL), op(CACHES), op(USER_ATTRIBUTES, "attrName1,...,attrNameN"));
        usageCache(RESET_LOST_PARTITIONS, CACHES);
        usageCache(FIND_AND_DELETE_GARBAGE, op(GROUPS), OP_NODE_ID, op(FindAndDeleteGarbageArg.DELETE));
        nl();
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private void cacheContention(GridClient client, CacheArguments cacheArgs) throws GridClientException {
        VisorContentionTaskArg taskArg = new VisorContentionTaskArg(
            cacheArgs.minQueueSize(), cacheArgs.maxPrint());

        UUID nodeId = cacheArgs.nodeId() == null ? BROADCAST_UUID : cacheArgs.nodeId();

        VisorContentionTaskResult res = executeTaskByNameOnNode(
            client, VisorContentionTask.class.getName(), taskArg, nodeId);

        if (!F.isEmpty(res.exceptions())) {
            log("Contention check failed on nodes:");

            for (Map.Entry<UUID, Exception> e : res.exceptions().entrySet()) {
                log("Node ID: " + e.getKey());

                log("Exception message:");
                log(e.getValue().getMessage());
                nl();
            }
        }

        for (ContentionInfo info : res.getInfos())
            info.print();
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private void cacheValidateIndexes(GridClient client, CacheArguments cacheArgs) throws GridClientException {
        VisorValidateIndexesTaskArg taskArg = new VisorValidateIndexesTaskArg(
            cacheArgs.caches(),
            cacheArgs.nodeId() != null ? Collections.singleton(cacheArgs.nodeId()) : null,
            cacheArgs.checkFirst(),
            cacheArgs.checkThrough()
        );

        VisorValidateIndexesTaskResult taskRes = executeTaskByNameOnNode(
            client, VALIDATE_INDEXES_TASK, taskArg, null);

        boolean errors = printErrors(taskRes.exceptions(), "Index validation failed on nodes:");

        for (Map.Entry<UUID, VisorValidateIndexesJobResult> nodeEntry : taskRes.results().entrySet()) {
            if (!nodeEntry.getValue().hasIssues())
                continue;

            errors = true;

            log("Index issues found on node " + nodeEntry.getKey() + ":");

            Collection<IndexIntegrityCheckIssue> integrityCheckFailures = nodeEntry.getValue().integrityCheckFailures();

            if (!integrityCheckFailures.isEmpty()) {
                for (IndexIntegrityCheckIssue is : integrityCheckFailures)
                    log(i(is));
            }

            Map<PartitionKey, ValidateIndexesPartitionResult> partRes = nodeEntry.getValue().partitionResult();

            for (Map.Entry<PartitionKey, ValidateIndexesPartitionResult> e : partRes.entrySet()) {
                ValidateIndexesPartitionResult res = e.getValue();

                if (!res.issues().isEmpty()) {
                    log(i(j(" ", e.getKey(), e.getValue())));

                    for (IndexValidationIssue is : res.issues())
                        log(i(is, 2));
                }
            }

            Map<String, ValidateIndexesPartitionResult> idxRes = nodeEntry.getValue().indexResult();

            for (Map.Entry<String, ValidateIndexesPartitionResult> e : idxRes.entrySet()) {
                ValidateIndexesPartitionResult res = e.getValue();

                if (!res.issues().isEmpty()) {
                    log(i(j(" ", "SQL Index", e.getKey(), e.getValue())));

                    for (IndexValidationIssue is : res.issues())
                        log(i(is, 2));
                }
            }
        }

        if (!errors)
            log("no issues found.");
        else
            log("issues found (listed above).");

        nl();
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private void cacheView(GridClient client, CacheArguments cacheArgs) throws GridClientException {
        VisorViewCacheTaskArg taskArg = new VisorViewCacheTaskArg(cacheArgs.regex(), cacheArgs.cacheCommand());

        VisorViewCacheTaskResult res = executeTaskByNameOnNode(
            client, VisorViewCacheTask.class.getName(), taskArg, cacheArgs.nodeId());

        if (cacheArgs.fullConfig() && cacheArgs.cacheCommand() == CACHES)
            cachesConfig(client, cacheArgs, res);
        else
            printCacheInfos(res.cacheInfos(), cacheArgs.cacheCommand());

    }

    /**
     * Executes appropriate version of idle_verify check. Old version will be used if there are old nodes in the
     * cluster.
     *
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private void cacheIdleVerify(GridClient client, CacheArguments cacheArgs) throws GridClientException {
        Collection<GridClientNode> nodes = client.compute().nodes(GridClientNode::connectable);

        boolean idleVerifyV2 = true;

        for (GridClientNode node : nodes) {
            String nodeVerStr = node.attribute(IgniteNodeAttributes.ATTR_BUILD_VER);

            IgniteProductVersion nodeVer = IgniteProductVersion.fromString(nodeVerStr);

            if (nodeVer.compareTo(VerifyBackupPartitionsTaskV2.V2_SINCE_VER) < 0) {
                idleVerifyV2 = false;

                break;
            }
        }

        if (cacheArgs.dump())
            cacheIdleVerifyDump(client, cacheArgs);
        else if (idleVerifyV2)
            cacheIdleVerifyV2(client, cacheArgs);
        else
            legacyCacheIdleVerify(client, cacheArgs);
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private VisorFindAndDeleteGarbageInPersistenceTaskResult findAndDeleteGarbage(
        GridClient client,
        CacheArguments cacheArgs
    ) throws GridClientException {
        VisorFindAndDeleteGarbageInPersistenceTaskArg taskArg = new VisorFindAndDeleteGarbageInPersistenceTaskArg(
            cacheArgs.groups(),
            cacheArgs.delete(),
            cacheArgs.nodeId() != null ? Collections.singleton(cacheArgs.nodeId()) : null
        );

        VisorFindAndDeleteGarbageInPersistenceTaskResult taskRes = executeTask(
            client, VisorFindAndDeleteGarbageInPersistenceTask.class, taskArg);

        printErrors(taskRes.exceptions(), "Scanning for garbage failed on nodes:");

        for (Map.Entry<UUID, VisorFindAndDeleteGarbageInPersistenceJobResult> nodeEntry : taskRes.result().entrySet()) {
            if (!nodeEntry.getValue().hasGarbage()) {
                log("Node "+ nodeEntry.getKey() + " - garbage not found.");

                continue;
            }

            log("Garbage found on node " + nodeEntry.getKey() + ":");

            VisorFindAndDeleteGarbageInPersistenceJobResult value = nodeEntry.getValue();

            Map<Integer, Map<Integer, Long>> grpPartErrorsCount = value.checkResult();

            if (!grpPartErrorsCount.isEmpty()) {
                for (Map.Entry<Integer, Map<Integer, Long>> entry : grpPartErrorsCount.entrySet()) {
                    for (Map.Entry<Integer, Long> e : entry.getValue().entrySet()) {
                        log(i("Group=" + entry.getKey() +
                            ", partition=" + e.getKey() +
                            ", count of keys=" + e.getValue()));
                    }
                }
            }

            nl();
        }

        return taskRes;
    }

    /**
     * @param exceptions Exception per node.
     * @param msg Message to print before errors if at least one presented.
     * @return True if found any error.
     */
    private boolean printErrors(Map<UUID, Exception> exceptions, String msg) {
        if (!F.isEmpty(exceptions)) {
            log(msg);

            for (Map.Entry<UUID, Exception> e : exceptions.entrySet()) {
                log(i("Node ID: " + e.getKey()));

                log(i("Exception message:"));
                log(i(e.getValue().getMessage(), 2));
                nl();
            }

            return true;
        }

        return false;
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private void legacyCacheIdleVerify(GridClient client, CacheArguments cacheArgs) throws GridClientException {
        VisorIdleVerifyTaskResult res = executeTask(
            client,
            VisorIdleVerifyTask.class,
            new VisorIdleVerifyTaskArg(cacheArgs.caches(), cacheArgs.excludeCaches(), cacheArgs.idleCheckCrc())
        );

        Map<PartitionKey, List<PartitionHashRecord>> conflicts = res.getConflicts();

        if (conflicts.isEmpty()) {
            log("idle_verify check has finished, no conflicts have been found.");
            nl();
        }
        else {
            log("idle_verify check has finished, found " + conflicts.size() + " conflict partitions.");
            nl();

            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : conflicts.entrySet()) {
                log("Conflict partition: " + entry.getKey());

                log("Partition instances: " + entry.getValue());
            }
        }
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private void cacheDistribution(GridClient client, CacheArguments cacheArgs) throws GridClientException {
        CacheDistributionTaskArg taskArg = new CacheDistributionTaskArg(cacheArgs.caches(), cacheArgs.getUserAttributes());

        UUID nodeId = cacheArgs.nodeId() == null ? BROADCAST_UUID : cacheArgs.nodeId();

        CacheDistributionTaskResult res = executeTaskByNameOnNode(client, CacheDistributionTask.class.getName(), taskArg, nodeId);

        res.print(System.out);
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param viewRes Cache view task result.
     */
    private void cachesConfig(GridClient client, CacheArguments cacheArgs,
        VisorViewCacheTaskResult viewRes) throws GridClientException {
        VisorCacheConfigurationCollectorTaskArg taskArg = new VisorCacheConfigurationCollectorTaskArg(cacheArgs.regex());

        UUID nodeId = cacheArgs.nodeId() == null ? BROADCAST_UUID : cacheArgs.nodeId();

        Map<String, VisorCacheConfiguration> res =
            executeTaskByNameOnNode(client, VisorCacheConfigurationCollectorTask.class.getName(), taskArg, nodeId);

        Map<String, Integer> cacheToMapped =
            viewRes.cacheInfos().stream().collect(Collectors.toMap(CacheInfo::getCacheName, CacheInfo::getMapped));

        printCachesConfig(res, cacheArgs.outputFormat(), cacheToMapped);
    }

    /**
     * Prints caches info.
     *
     * @param infos Caches info.
     * @param cmd Command.
     */
    private void printCacheInfos(Collection<CacheInfo> infos, VisorViewCacheCmd cmd) {
        for (CacheInfo info : infos) {
            Map<String, Object> map = info.toMap(cmd);

            SB sb = new SB("[");

            for (Map.Entry<String, Object> e : map.entrySet())
                sb.a(e.getKey()).a("=").a(e.getValue()).a(", ");

            sb.setLength(sb.length() - 2);

            sb.a("]");

            log(sb.toString());
        }
    }

    /**
     * Prints caches config.
     *
     * @param caches Caches config.
     * @param outputFormat Output format.
     * @param cacheToMapped Map cache name to mapped.
     */
    private void printCachesConfig(
        Map<String, VisorCacheConfiguration> caches,
        OutputFormat outputFormat,
        Map<String, Integer> cacheToMapped
    ) {

        for (Map.Entry<String, VisorCacheConfiguration> entry : caches.entrySet()) {
            String cacheName = entry.getKey();

            switch (outputFormat) {
                case MULTI_LINE:
                    Map<String, Object> params = mapToPairs(entry.getValue());

                    params.put("Mapped", cacheToMapped.get(cacheName));

                    log("[cache = '%s']%n", cacheName);

                    for (Map.Entry<String, Object> innerEntry : params.entrySet())
                        log("%s: %s%n", innerEntry.getKey(), innerEntry.getValue());

                    nl();

                    break;

                default:
                    int mapped = cacheToMapped.get(cacheName);

                    log("%s: %s %s=%s%n", entry.getKey(), toString(entry.getValue()), "mapped", mapped);

                    break;
            }
        }
    }

    /**
     * Invokes toString() method and cuts class name from result string.
     *
     * @param cfg Visor cache configuration for invocation.
     * @return String representation without class name in begin of string.
     */
    private String toString(VisorCacheConfiguration cfg) {
        return cfg.toString().substring(cfg.getClass().getSimpleName().length() + 1);
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private void cacheResetLostPartitions(GridClient client, CacheArguments cacheArgs) throws GridClientException {
        CacheResetLostPartitionsTaskArg taskArg = new CacheResetLostPartitionsTaskArg(cacheArgs.caches());

        CacheResetLostPartitionsTaskResult res = executeTaskByNameOnNode(client, CacheResetLostPartitionsTask.class.getName(), taskArg, null);

        res.print(System.out);
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private void cacheIdleVerifyDump(GridClient client, CacheArguments cacheArgs) throws GridClientException {
        VisorIdleVerifyDumpTaskArg arg = new VisorIdleVerifyDumpTaskArg(
            cacheArgs.caches(),
            cacheArgs.excludeCaches(),
            cacheArgs.isSkipZeros(),
            cacheArgs.getCacheFilterEnum(),
            cacheArgs.idleCheckCrc()
        );

        String path = executeTask(client, VisorIdleVerifyDumpTask.class, arg);

        log("VisorIdleVerifyDumpTask successfully written output to '" + path + "'");
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     */
    private void cacheIdleVerifyV2(GridClient client, CacheArguments cacheArgs) throws GridClientException {
        IdleVerifyResultV2 res = executeTask(
            client,
            VisorIdleVerifyTaskV2.class,
            new VisorIdleVerifyTaskArg(cacheArgs.caches(), cacheArgs.excludeCaches(), cacheArgs.idleCheckCrc())
        );

        res.print(System.out::print);
    }

    /**
     * Change baseline.
     *
     * @param client Client.
     * @param baselineArgs Baseline action arguments.
     * @throws Throwable If failed to execute baseline action.
     */
    private void baseline(GridClient client, BaselineArguments baselineArgs) throws Throwable {
        try {
            VisorBaselineTaskResult res = executeTask(client, VisorBaselineTask.class, toVisorArguments(baselineArgs));

            baselinePrint0(res);
        }
        catch (Throwable e) {
            log("Failed to execute baseline command='" + baselineArgs.getCmd().text() + "'", e);

            throw e;
        }
    }

    /**
     * Prepare task argument.
     *
     * @param args Argument from command line.
     * @return Task argument.
     */
    private VisorBaselineTaskArg toVisorArguments(BaselineArguments args) {
        VisorBaselineAutoAdjustSettings settings = args.getCmd() == BaselineCommand.AUTO_ADJUST
            ? new VisorBaselineAutoAdjustSettings(args.getEnableAutoAdjust(), args.getSoftBaselineTimeout())
            : null;

        return new VisorBaselineTaskArg(toVisorOperation(args.getCmd()), args.getTopVer(), args.getConsistentIds(), settings);
    }

    /**
     * Convert {@link BaselineCommand} to {@link VisorBaselineOperation}.
     *
     * @param baselineCommand Baseline command for convertion.
     * @return {@link VisorBaselineOperation}.
     */
    private VisorBaselineOperation toVisorOperation(BaselineCommand baselineCommand) {
        switch (baselineCommand) {
            case ADD:
                return VisorBaselineOperation.ADD;
            case AUTO_ADJUST:
                return VisorBaselineOperation.AUTOADJUST;
            case REMOVE:
                return VisorBaselineOperation.REMOVE;
            case SET:
                return VisorBaselineOperation.SET;
            case VERSION:
                return VisorBaselineOperation.VERSION;
            case COLLECT:
                return VisorBaselineOperation.COLLECT;
        }

        return null;
    }

    /**
     * @param s String of consisted ids delimited by comma.
     * @return List of consistent ids.
     */
    private List<String> getConsistentIds(String s) {
        if (F.isEmpty(s))
            throw new IllegalArgumentException("Empty list of consistent IDs");

        List<String> consistentIds = new ArrayList<>();

        for (String consistentId : s.split(","))
            consistentIds.add(consistentId.trim());

        return consistentIds;
    }

    /**
     * Print baseline topology.
     *
     * @param res Task result with baseline topology.
     */
    private void baselinePrint0(VisorBaselineTaskResult res) {
        log("Cluster state: " + (res.isActive() ? "active" : "inactive"));
        log("Current topology version: " + res.getTopologyVersion());
        VisorBaselineAutoAdjustSettings autoAdjustSettings = res.getAutoAdjustSettings();

        if (autoAdjustSettings != null) {
            log("Baseline auto adjustment " + (TRUE.equals(autoAdjustSettings.getEnabled()) ? "enabled" : "disabled")
                + ": softTimeout=" + autoAdjustSettings.getSoftTimeout()
            );
        }

        if (autoAdjustSettings.enabled) {
            if (res.isBaselineAdjustInProgress())
                log("Baseline auto-adjust is in progress");
            else if (res.getRemainingTimeToBaselineAdjust() < 0)
                log("Baseline auto-adjust are not scheduled");
            else
                log("Baseline auto-adjust will happen in '" + res.getRemainingTimeToBaselineAdjust() + "' ms");
        }

        nl();

        Map<String, VisorBaselineNode> baseline = res.getBaseline();

        Map<String, VisorBaselineNode> srvs = res.getServers();

        // if task runs on a node with VisorBaselineNode of old version (V1) we'll get order=null for all nodes.

        String crdStr = srvs.values().stream()
            // check for not null
            .filter(node -> node.getOrder() != null)
            .min(Comparator.comparing(VisorBaselineNode::getOrder))
            // format
            .map(crd -> " (Coordinator: ConsistentId=" + crd.getConsistentId() + ", Order=" + crd.getOrder() + ")")
            .orElse("");

        log("Current topology version: " + res.getTopologyVersion() + crdStr);
        nl();

        if (F.isEmpty(baseline))
            log("Baseline nodes not found.");
        else {
            log("Baseline nodes:");

            for (VisorBaselineNode node : baseline.values()) {
                VisorBaselineNode srvNode = srvs.get(node.getConsistentId());

                String state = ", State=" + (srvNode != null ? "ONLINE" : "OFFLINE");

                String order = srvNode != null ? ", Order=" + srvNode.getOrder() : "";

                log(i("ConsistentId=" + node.getConsistentId() + state + order, 2));
            }

            log(DELIM);
            log("Number of baseline nodes: " + baseline.size());

            nl();

            List<VisorBaselineNode> others = new ArrayList<>();

            for (VisorBaselineNode node : srvs.values()) {
                if (!baseline.containsKey(node.getConsistentId()))
                    others.add(node);
            }

            if (F.isEmpty(others))
                log("Other nodes not found.");
            else {
                log("Other nodes:");

                for (VisorBaselineNode node : others)
                    log(i("ConsistentId=" + node.getConsistentId() + ", Order=" + node.getOrder(), 2));

                log("Number of other nodes: " + others.size());
            }
        }
    }

    /**
     * Dump transactions information.
     *
     * @param client Client.
     * @param arg Transaction search arguments
     */
    private void transactions(GridClient client, VisorTxTaskArg arg) throws GridClientException {
        try {
            if (arg.getOperation() == VisorTxOperation.INFO) {
                transactionInfo(client, arg);

                return;
            }

            Map<ClusterNode, VisorTxTaskResult> res = executeTask(client, VisorTxTask.class, arg);

            lastOperationRes = res;

            if (res.isEmpty())
                log("Nothing found.");
            else if (arg.getOperation() == VisorTxOperation.KILL)
                log("Killed transactions:");
            else
                log("Matching transactions:");

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
                if (entry.getValue().getInfos().isEmpty())
                    continue;

                ClusterNode key = entry.getKey();

                log(nodeDescription(key));

                for (VisorTxInfo info : entry.getValue().getInfos())
                    log(info.toUserString());
            }
        }
        catch (Throwable e) {
            log("Failed to perform operation.");

            throw e;
        }
    }

    /**
     * Provides text descrition of a cluster node.
     *
     * @param node Node.
     */
    private static String nodeDescription(ClusterNode node) {
        return node.getClass().getSimpleName() + " [id=" + node.id() +
            ", addrs=" + node.addresses() +
            ", order=" + node.order() +
            ", ver=" + node.version() +
            ", isClient=" + node.isClient() +
            ", consistentId=" + node.consistentId() +
            "]";
    }

    /**
     * Executes --tx --info command.
     *
     * @param client Client.
     * @param arg Argument.
     */
    private void transactionInfo(GridClient client, VisorTxTaskArg arg) throws GridClientException {
        checkFeatureSupportedByCluster(client, IgniteFeatures.TX_INFO_COMMAND, true);

        GridCacheVersion nearXidVer = executeTask(client, FetchNearXidVersionTask.class, arg.txInfoArgument());

        boolean histMode = false;

        if (nearXidVer != null) {
            log("Resolved transaction near XID version: " + nearXidVer);

            arg.txInfoArgument(new TxVerboseId(null, nearXidVer));
        }
        else {
            log("Active transactions not found.");

            if (arg.txInfoArgument().gridCacheVersion() != null) {
                log("Will try to peek history to find out whether transaction was committed / rolled back.");

                histMode = true;
            }
            else {
                log("You can specify transaction in GridCacheVersion format in order to peek history " +
                    "to find out whether transaction was committed / rolled back.");

                return;
            }
        }

        Map<ClusterNode, VisorTxTaskResult> res = executeTask(client, VisorTxTask.class, arg);

        lastOperationRes = res;

        if (histMode)
            printTxInfoHistoricalResult(res);
        else
            printTxInfoResult(res);


    }

    /**
     * Prints result of --tx --info command to output.
     *
     * @param res Response.
     */
    private void printTxInfoResult(Map<ClusterNode, VisorTxTaskResult> res) {
        String lb = null;

        Map<Integer, String> usedCaches = new HashMap<>();
        Map<Integer, String> usedCacheGroups = new HashMap<>();
        VisorTxInfo firstInfo = null;
        TxVerboseInfo firstVerboseInfo = null;
        Set<TransactionState> states = new HashSet<>();

        for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
            for (VisorTxInfo info : entry.getValue().getInfos()) {
                assert info.getTxVerboseInfo() != null;

                if (lb == null)
                    lb = info.getLabel();

                if (firstInfo == null) {
                    firstInfo = info;
                    firstVerboseInfo = info.getTxVerboseInfo();
                }

                usedCaches.putAll(info.getTxVerboseInfo().usedCaches());
                usedCacheGroups.putAll(info.getTxVerboseInfo().usedCacheGroups());
                states.add(info.getState());
            }
        }

        String indent = "";

        nl();
        log(indent + "Transaction detailed info:");

        printTransactionDetailedInfo(
            res, usedCaches, usedCacheGroups, firstInfo, firstVerboseInfo, states, indent + DOUBLE_INDENT);
    }

    /**
     * Prints detailed info about transaction to output.
     *
     * @param res Response.
     * @param usedCaches Used caches.
     * @param usedCacheGroups Used cache groups.
     * @param firstInfo First info.
     * @param firstVerboseInfo First verbose info.
     * @param states States.
     * @param indent Indent.
     */
    private void printTransactionDetailedInfo(Map<ClusterNode, VisorTxTaskResult> res, Map<Integer, String> usedCaches,
        Map<Integer, String> usedCacheGroups, VisorTxInfo firstInfo, TxVerboseInfo firstVerboseInfo,
        Set<TransactionState> states, String indent) {
        log(indent + "Near XID version: " + firstVerboseInfo.nearXidVersion());
        log(indent + "Near XID version (UUID): " + firstInfo.getNearXid());
        log(indent + "Isolation: " + firstInfo.getIsolation());
        log(indent + "Concurrency: " + firstInfo.getConcurrency());
        log(indent + "Timeout: " + firstInfo.getTimeout());
        log(indent + "Initiator node: " + firstVerboseInfo.nearNodeId());
        log(indent + "Initiator node (consistent ID): " + firstVerboseInfo.nearNodeConsistentId());
        log(indent + "Label: " + firstInfo.getLabel());
        log(indent + "Topology version: " + firstInfo.getTopologyVersion());
        log(indent + "Used caches (ID to name): " + usedCaches);
        log(indent + "Used cache groups (ID to name): " + usedCacheGroups);
        log(indent + "States across the cluster: " + states);
        log(indent + "Transaction topology: ");

        printTransactionTopology(res, indent + DOUBLE_INDENT);
    }

    /**
     * Prints transaction topology to output.
     *
     * @param res Response.
     * @param indent Indent.
     */
    private void printTransactionTopology(Map<ClusterNode, VisorTxTaskResult> res, String indent) {
        for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
            log(indent + nodeDescription(entry.getKey()) + ':');

            printTransactionMappings(indent + DOUBLE_INDENT, entry);
        }
    }

    /**
     * Prints transaction mappings for specific cluster node to output.
     *
     * @param indent Indent.
     * @param entry Entry.
     */
    private void printTransactionMappings(String indent, Map.Entry<ClusterNode, VisorTxTaskResult> entry) {
        for (VisorTxInfo info : entry.getValue().getInfos()) {
            TxVerboseInfo verboseInfo = info.getTxVerboseInfo();

            if (verboseInfo != null) {
                log(indent + "Mapping [type=" + verboseInfo.txMappingType() + "]:");

                printTransactionMapping(indent + DOUBLE_INDENT, info, verboseInfo);
            }
            else {
                log(indent + "Mapping [type=HISTORICAL]:");

                log(indent + DOUBLE_INDENT + "State: " + info.getState());
            }
        }
    }

    /**
     * Prints specific transaction mapping to output.
     *
     * @param indent Indent.
     * @param info Info.
     * @param verboseInfo Verbose info.
     */
    private void printTransactionMapping(String indent, VisorTxInfo info, TxVerboseInfo verboseInfo) {
        log(indent + "XID version (UUID): " + info.getXid());
        log(indent + "State: " + info.getState());

        if (verboseInfo.txMappingType() == TxMappingType.REMOTE) {
            log(indent + "Primary node: " + verboseInfo.dhtNodeId());
            log(indent + "Primary node (consistent ID): " + verboseInfo.dhtNodeConsistentId());
        }

        if (!F.isEmpty(verboseInfo.localTxKeys())) {
            log(indent + "Mapped keys:");

            printTransactionKeys(indent + DOUBLE_INDENT, verboseInfo);
        }
    }

    /**
     * Prints keys of specific transaction mapping to output.
     *
     * @param indent Indent.
     * @param verboseInfo Verbose info.
     */
    private void printTransactionKeys(String indent, TxVerboseInfo verboseInfo) {
        for (TxVerboseKey txVerboseKey : verboseInfo.localTxKeys()) {
            log(indent + (txVerboseKey.read() ? "Read" : "Write") +
                " [lock=" + txVerboseKey.lockType() + "]: " + txVerboseKey.txKey());

            if (txVerboseKey.lockType() == TxKeyLockType.AWAITS_LOCK)
                log(indent + DOUBLE_INDENT + "Lock owner XID: " + txVerboseKey.ownerVersion());
        }
    }

    /**
     * Prints results of --tx --info to output in case requested transaction is not active.
     *
     * @param res Response.
     */
    private void printTxInfoHistoricalResult(Map<ClusterNode, VisorTxTaskResult> res) {
        if (F.isEmpty(res))
            log("Transaction was not found in history across the cluster.");
        else {
            log("Transaction was found in completed versions history of the following nodes:");

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
                log(DOUBLE_INDENT + nodeDescription(entry.getKey()) + ':');
                log(DOUBLE_INDENT + DOUBLE_INDENT + "State: " + entry.getValue().getInfos().get(0).getState());
            }
        }
    }

    /**
     * Checks that all cluster nodes support specified feature.
     *
     * @param client Client.
     * @param feature Feature.
     * @param validateClientNodes Whether client nodes should be checked as well.
     */
    private static void checkFeatureSupportedByCluster(
        GridClient client,
        IgniteFeatures feature,
        boolean validateClientNodes
    ) throws GridClientException {
        Collection<GridClientNode> nodes = validateClientNodes ?
            client.compute().nodes() :
            client.compute().nodes(GridClientNode::connectable);

        for (GridClientNode node : nodes) {
            byte[] featuresAttrBytes = node.attribute(IgniteNodeAttributes.ATTR_IGNITE_FEATURES);

            if (!IgniteFeatures.nodeSupports(featuresAttrBytes, feature)) {
                throw new IllegalStateException("Failed to execute command: cluster contains node that " +
                    "doesn't support feature [nodeId=" + node.nodeId() + ", feature=" + feature + ']');
            }
        }
    }

    /**
     * Execute WAL command.
     *
     * @param client Client.
     * @param walAct WAL action to execute.
     * @param walArgs WAL args.
     * @throws Throwable If failed to execute wal action.
     */
    private void wal(GridClient client, String walAct, String walArgs) throws Throwable {
        switch (walAct) {
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
     * @param walArgs WAL args.
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
     * Prepare WAL task argument.
     *
     * @param op Operation.
     * @param s Argument from command line.
     * @return Task argument.
     */
    private VisorWalTaskArg walArg(VisorWalTaskOperation op, String s) {
        List<String> consistentIds = null;

        if (!F.isEmpty(s)) {
            consistentIds = new ArrayList<>();

            for (String consistentId : s.split(","))
                consistentIds.add(consistentId.trim());
        }

        switch (op) {
            case DELETE_UNUSED_WAL_SEGMENTS:
            case PRINT_UNUSED_WAL_SEGMENTS:
                return new VisorWalTaskArg(op, consistentIds);

            default:
                return new VisorWalTaskArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS, consistentIds);
        }

    }

    /**
     * Print list of unused wal segments.
     *
     * @param taskRes Task result with baseline topology.
     */
    private void printUnusedWalSegments0(VisorWalTaskResult taskRes) {
        log("Unused wal segments per node:");
        nl();

        Map<String, Collection<String>> res = taskRes.results();
        Map<String, Exception> failRes = taskRes.exceptions();
        Map<String, VisorClusterNode> nodesInfo = taskRes.getNodesInfo();

        for (Map.Entry<String, Collection<String>> entry : res.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            log("Node=" + node.getConsistentId());
            log(i("addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames()), 2));

            for (String fileName : entry.getValue())
                log(i(fileName));

            nl();
        }

        for (Map.Entry<String, Exception> entry : failRes.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            log("Node=" + node.getConsistentId());
            log(i("addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames())), 2);
            log(i("failed with error: " + entry.getValue().getMessage()));
            nl();
        }
    }

    /**
     * Print list of unused wal segments.
     *
     * @param taskRes Task result with baseline topology.
     */
    private void printDeleteWalSegments0(VisorWalTaskResult taskRes) {
        log("WAL segments deleted for nodes:");
        nl();

        Map<String, Collection<String>> res = taskRes.results();
        Map<String, Exception> errors = taskRes.exceptions();
        Map<String, VisorClusterNode> nodesInfo = taskRes.getNodesInfo();

        for (Map.Entry<String, Collection<String>> entry : res.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            log("Node=" + node.getConsistentId());
            log(i("addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames())), 2);
            nl();
        }

        for (Map.Entry<String, Exception> entry : errors.entrySet()) {
            VisorClusterNode node = nodesInfo.get(entry.getKey());

            log("Node=" + node.getConsistentId());
            log(i("addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames())), 2);
            log(i("failed with error: " + entry.getValue().getMessage()));
            nl();
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
     * @param args Arguments.
     */
    private void usage(String desc, Command cmd, String... args) {
        log(desc);
        log(i(j(" ", UTILITY_NAME, cmd, j(" ", args)), 2));
        nl();
    }

    /**
     * Print cache command usage with default indention.
     *
     * @param cmd Cache command.
     * @param args Cache command arguments.
     */
    private void usageCache(CacheCommand cmd, String... args) {
        usageCache(1, cmd, args);
    }

    /**
     * Print cache command usage.
     *
     * @param indentsNum Number of indents.
     * @param cmd Cache command.
     * @param args Cache command arguments.
     */
    private void usageCache(int indentsNum, CacheCommand cmd, String... args) {
        log(i(DELIM, indentsNum));
        nl();
        log(i(j(" ", CACHE, cmd, j(" ", args)), indentsNum++));
        nl();
        log(i(getCacheSubcommandDesc(cmd), indentsNum));
        nl();

        Map<String, String> paramsDesc = createCacheArgsDesc(cmd);

        if (!paramsDesc.isEmpty()) {
            log(i("Parameters:", indentsNum));

            usageCacheParams(paramsDesc, indentsNum + 1);

            nl();
        }
    }

    /**
     * Print cache command arguments usage.
     *
     * @param paramsDesc Cache command arguments description.
     * @param indentsNum Number of indents.
     */
    private void usageCacheParams(Map<String, String> paramsDesc, int indentsNum) {
        int maxParamLen = paramsDesc.keySet().stream().max(Comparator.comparingInt(String::length)).get().length();

        for (Map.Entry<String, String> param : paramsDesc.entrySet())
            log(i(extendToLen(param.getKey(), maxParamLen) + INDENT + "- " + param.getValue(), indentsNum));
    }

    /**
     * Appends spaces to end of input string for extending to needed length.
     *
     * @param s Input string.
     * @param targetLen Needed length.
     * @return String with appended spaces on the end.
     */
    private String extendToLen(String s, int targetLen) {
        assert targetLen >= 0;
        assert s.length() <= targetLen;

        if (s.length() == targetLen)
            return s;

        SB sb = new SB(targetLen);

        sb.a(s);

        for (int i = 0; i < targetLen - s.length(); i++)
            sb.a(" ");

        return sb.toString();
    }

    /**
     * Gets cache command description by cache command.
     *
     * @param cmd Cache command.
     * @return Cache command description.
     */
    private String getCacheSubcommandDesc(CacheCommand cmd) {
        switch (cmd) {
            case LIST:
                return "Show information about caches, groups or sequences that match a regular expression. When executed without parameters, this subcommand prints the list of caches.";

            case CONTENTION:
                return "Show the keys that are point of contention for multiple transactions.";

            case IDLE_VERIFY:
                return "Verify counters and hash sums of primary and backup partitions for the specified caches/cache groups on an idle cluster and print out the differences, if any. " +
                    "Cache filtering options configure the set of caches that will be processed by " + IDLE_VERIFY + " command. " +
                    "Default value for the set of cache names (or cache group names) is all cache groups. Default value for " + EXCLUDE_CACHES + " is empty set. " +
                    "Default value for " + CACHE_FILTER + " is no filtering. Therefore, the set of all caches is sequently filtered by cache name " +
                    "regexps, by cache type and after all by exclude regexps.";

            case VALIDATE_INDEXES:
                return "Validate indexes on an idle cluster and print out the keys that are missing in the indexes.";

            case DISTRIBUTION:
                return "Prints the information about partition distribution.";

            case RESET_LOST_PARTITIONS:
                return "Reset the state of lost partitions for the specified caches.";

            case FIND_AND_DELETE_GARBAGE:
                return "Find and optionally delete garbage from shared cache groups which could be left after cache destroy.";

            default:
                throw new IllegalArgumentException("Unknown command: " + cmd);
        }
    }

    /**
     * Gets cache command arguments description by cache command.
     *
     * @param cmd Cache command.
     * @return Cache command arguments description.
     */
    private Map<String, String> createCacheArgsDesc(CacheCommand cmd) {
        Map<String, String> map = U.newLinkedHashMap(16);
        switch (cmd) {
            case LIST:
                map.put(CONFIG.toString(), "print all configuration parameters for each cache.");
                map.put(OUTPUT_FORMAT + " " + MULTI_LINE, "print configuration parameters per line. This option has effect only when used with " + CONFIG + " and without " + op(or(GROUP, SEQUENCE)) + ".");
                map.put(GROUP.toString(), "print information about groups.");
                map.put(SEQUENCE.toString(), "print information about sequences.");

                break;
            case VALIDATE_INDEXES:
                map.put(CHECK_FIRST + " N", "validate only the first N keys");
                map.put(CHECK_THROUGH + " K", "validate every Kth key");

                break;

            case IDLE_VERIFY:
                map.put(CHECK_CRC.toString(), "check the CRC-sum of pages stored on disk before verifying data consistency in partitions between primary and backup nodes.");

                break;

            case FIND_AND_DELETE_GARBAGE:
                map.put(FindAndDeleteGarbageArg.DELETE.toString(), "remove found garbage or not");

                break;
        }
        return map;
    }

    /**
     * Join input parameters with space and wrap optional braces {@code []}.
     *
     * @param params Other input parameter.
     * @return Joined parameters wrapped optional braces.
     */
    private static String op(Object... params) {
        return j(new SB(), "[", " ", params).a("]").toString();
    }

    /**
     * Join input parameters with specified {@code delimeter} between them.
     *
     * @param delimeter Specified delimeter.
     * @param params Other input parameter.
     * @return Joined paramaters with specified {@code delimeter}.
     */
    private static String j(String delimeter, Object... params) {
        return j(new SB(), "", delimeter, params).toString();
    }

    /**
     * Join input parameters with specified {@code delimeter} between them and append to the end {@code delimeter}.
     *
     * @param sb Specified string builder.
     * @param sbDelimeter Delimeter between {@code sb} and appended {@code param}.
     * @param delimeter Specified delimeter.
     * @param params Other input parameter.
     * @return SB with appended to the end joined paramaters with specified {@code delimeter}.
     */
    private static SB j(SB sb, String sbDelimeter, String delimeter, Object... params) {
        if (!F.isEmpty(params)) {
            sb.a(sbDelimeter);

            for (Object par : params)
                sb.a(par).a(delimeter);

            sb.setLength(sb.length() - delimeter.length());
        }

        return sb;
    }

    /**
     * Concatenates input parameters to single string with OR delimiter {@code |}.
     *
     * @param params Remaining parameters.
     * @return Concatenated string.
     */
    private static String or(Object... params) {
        return j("|", params);
    }

    /**
     * Join input parameters with space and wrap grouping braces {@code ()}.
     *
     * @param params Input parameter.
     * @return Joined parameters wrapped grouped braces.
     */
    private static String g(Object... params) {
        return j(new SB(), "(", " ", params).a(")").toString();
    }

    /**
     * Extract next argument.
     *
     * @param err Error message.
     * @return Next argument value.
     */
    private String nextArg(String err) {
        if (peekedArg != null) {
            String res = peekedArg;

            peekedArg = null;

            return res;
        }

        if (argsIt.hasNext())
            return argsIt.next();

        throw new IllegalArgumentException(err);
    }

    /**
     * Returns the next argument in the iteration, without advancing the iteration.
     *
     * @return Next argument value or {@code null} if no next argument.
     */
    private String peekNextArg() {
        if (peekedArg == null && argsIt.hasNext())
            peekedArg = argsIt.next();

        return peekedArg;
    }

    /**
     * Parses and validates arguments.
     *
     * @param rawArgs Array of arguments.
     * @return Arguments bean.
     * @throws IllegalArgumentException In case arguments aren't valid.
     */
    Arguments parseAndValidate(List<String> rawArgs) {
        String host = DFLT_HOST;

        String port = DFLT_PORT;

        String user = null;

        String pwd = null;

        Long pingInterval = DFLT_PING_INTERVAL;

        Long pingTimeout = DFLT_PING_TIMEOUT;

        String walAct = "";

        String walArgs = "";

        boolean autoConfirmation = false;

        CacheArguments cacheArgs = null;

        BaselineArguments baselineArgs = null;

        List<Command> commands = new ArrayList<>();

        initArgIterator(rawArgs);

        VisorTxTaskArg txArgs = null;

        String sslProtocol = DFLT_SSL_PROTOCOL;

        String sslCipherSuites = "";

        String sslKeyAlgorithm = SslContextFactory.DFLT_KEY_ALGORITHM;

        String sslKeyStoreType = SslContextFactory.DFLT_STORE_TYPE;

        String sslKeyStorePath = null;

        char sslKeyStorePassword[] = null;

        String sslTrustStoreType = SslContextFactory.DFLT_STORE_TYPE;

        String sslTrustStorePath = null;

        char sslTrustStorePassword[] = null;

        final String pwdArgWarnFmt = "Warning: %s is insecure. " +
            "Whenever possible, use interactive prompt for password (just discard %s option).";

        while (hasNextArg()) {
            String str = nextArg("").toLowerCase();

            Command cmd = Command.of(str);

            if (cmd != null) {
                switch (cmd) {
                    case ACTIVATE:
                    case DEACTIVATE:
                    case STATE:
                        commands.add(cmd);

                        break;

                    case TX:
                        commands.add(TX);

                        txArgs = parseTransactionArguments();

                        break;

                    case BASELINE:
                        commands.add(BASELINE);

                        baselineArgs = parseAndValidateBaselineArgs();

                        break;

                    case CACHE:
                        commands.add(CACHE);

                        cacheArgs = parseAndValidateCacheArgs();

                        break;

                    case WAL:
                        if (!enableExperimental)
                            throw new IllegalArgumentException("Experimental command is disabled.");

                        commands.add(WAL);

                        str = nextArg("Expected arguments for " + WAL.text());

                        walAct = str.toLowerCase();

                        if (WAL_PRINT.equals(walAct) || WAL_DELETE.equals(walAct))
                            walArgs = (str = peekNextArg()) != null && !isCommandOrOption(str)
                                ? nextArg("Unexpected argument for " + WAL.text() + ": " + walAct)
                                : "";
                        else
                            throw new IllegalArgumentException("Unexpected action " + walAct + " for " + WAL.text());

                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected command: " + str);
                }
            }
            else {
                switch (str) {
                    case CMD_HOST:
                        host = nextArg("Expected host name");

                        break;

                    case CMD_PORT:
                        port = nextArg("Expected port number");

                        try {
                            int p = Integer.parseInt(port);

                            if (p <= 0 || p > 65535)
                                throw new IllegalArgumentException("Invalid value for port: " + port);
                        }
                        catch (NumberFormatException ignored) {
                            throw new IllegalArgumentException("Invalid value for port: " + port);
                        }

                        break;

                    case CMD_PING_INTERVAL:
                        pingInterval = getPingParam("Expected ping interval", "Invalid value for ping interval");

                        break;

                    case CMD_PING_TIMEOUT:
                        pingTimeout = getPingParam("Expected ping timeout", "Invalid value for ping timeout");

                        break;

                    case CMD_USER:
                        user = nextArg("Expected user name");

                        break;

                    case CMD_PASSWORD:
                        pwd = nextArg("Expected password");

                        log(format(pwdArgWarnFmt, CMD_PASSWORD, CMD_PASSWORD));

                        break;

                    case CMD_SSL_PROTOCOL:
                        sslProtocol = nextArg("Expected SSL protocol");

                        break;

                    case CMD_SSL_CIPHER_SUITES:
                        sslCipherSuites = nextArg("Expected SSL cipher suites");

                        break;

                    case CMD_SSL_KEY_ALGORITHM:
                        sslKeyAlgorithm = nextArg("Expected SSL key algorithm");

                        break;

                    case CMD_KEYSTORE:
                        sslKeyStorePath = nextArg("Expected SSL key store path");

                        break;

                    case CMD_KEYSTORE_PASSWORD:
                        sslKeyStorePassword = nextArg("Expected SSL key store password").toCharArray();

                        log(format(pwdArgWarnFmt, CMD_KEYSTORE_PASSWORD, CMD_KEYSTORE_PASSWORD));

                        break;

                    case CMD_KEYSTORE_TYPE:
                        sslKeyStoreType = nextArg("Expected SSL key store type");

                        break;

                    case CMD_TRUSTSTORE:
                        sslTrustStorePath = nextArg("Expected SSL trust store path");

                        break;

                    case CMD_TRUSTSTORE_PASSWORD:
                        sslTrustStorePassword = nextArg("Expected SSL trust store password").toCharArray();

                        log(format(pwdArgWarnFmt, CMD_TRUSTSTORE_PASSWORD, CMD_TRUSTSTORE_PASSWORD));

                        break;

                    case CMD_TRUSTSTORE_TYPE:
                        sslTrustStoreType = nextArg("Expected SSL trust store type");

                        break;

                    case CMD_AUTO_CONFIRMATION:
                        autoConfirmation = true;

                        break;

                    default:
                        throw new IllegalArgumentException("Unexpected argument: " + str);
                }
            }
        }

        int sz = commands.size();

        if (sz < 1)
            throw new IllegalArgumentException("No action was specified");

        if (sz > 1)
            throw new IllegalArgumentException("Only one action can be specified, but found: " + sz);

        Command cmd = commands.get(0);

        return new Arguments(cmd, host, port, user, pwd,
            baselineArgs,
            txArgs, cacheArgs,
            walAct, walArgs,
            pingTimeout, pingInterval, autoConfirmation,
            sslProtocol, sslCipherSuites,
            sslKeyAlgorithm, sslKeyStorePath, sslKeyStorePassword, sslKeyStoreType,
            sslTrustStorePath, sslTrustStorePassword, sslTrustStoreType);
    }

    /**
     * Parses and validates baseline arguments.
     *
     * @return --baseline subcommand arguments in case validation is successful.
     */
    private BaselineArguments parseAndValidateBaselineArgs() {
        if (!hasNextSubArg())
            return new BaselineArguments.Builder(BaselineCommand.COLLECT).build();

        BaselineCommand cmd = of(nextArg("Expected baseline action"));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct baseline action");

        BaselineArguments.Builder baselineArgs = new BaselineArguments.Builder(cmd);

        switch (cmd) {
            case ADD:
            case REMOVE:
            case SET:
                return baselineArgs
                    .withConsistentIds(getConsistentIds(nextArg("Expected list of consistent ids")))
                    .build();
            case VERSION:
                return baselineArgs
                    .withTopVer(nextLongArg("topology version"))
                    .build();
            case AUTO_ADJUST:
                do {
                    AutoAdjustCommandArg autoAdjustArg = CommandArgUtils.of(
                        nextArg("Expected one of auto-adjust arguments"), AutoAdjustCommandArg.class
                    );

                    if (autoAdjustArg == null)
                        throw new IllegalArgumentException("Expected one of auto-adjust arguments");

                    if (autoAdjustArg == AutoAdjustCommandArg.ENABLE || autoAdjustArg == AutoAdjustCommandArg.DISABLE)
                        baselineArgs.withEnable(autoAdjustArg == AutoAdjustCommandArg.ENABLE);

                    if (autoAdjustArg == AutoAdjustCommandArg.TIMEOUT)
                        baselineArgs.withSoftBaselineTimeout(nextLongArg("soft timeout"));
                }
                while (hasNextSubArg());

                return baselineArgs.build();
        }

        return baselineArgs.build();
    }

    /**
     * Parses and validates cache arguments.
     *
     * @return --cache subcommand arguments in case validation is successful.
     */
    private CacheArguments parseAndValidateCacheArgs() {
        if (!hasNextSubArg()) {
            throw new IllegalArgumentException("Arguments are expected for --cache subcommand, " +
                "run --cache help for more info.");
        }

        CacheArguments cacheArgs = new CacheArguments();

        String str = nextArg("").toLowerCase();

        CacheCommand cmd = CacheCommand.of(str);

        if (cmd == null)
            cmd = CacheCommand.HELP;

        cacheArgs.command(cmd);

        switch (cmd) {
            case HELP:
                break;

            case IDLE_VERIFY:
                int idleVerifyArgsCnt = 5;

                while (hasNextSubArg() && idleVerifyArgsCnt-- > 0) {
                    String nextArg = nextArg("");

                    IdleVerifyCommandArg arg = CommandArgUtils.of(nextArg, IdleVerifyCommandArg.class);

                    if (arg == null)
                        parseCacheNames(nextArg, cacheArgs);
                    else {
                        switch (arg) {
                            case DUMP:
                                cacheArgs.dump(true);

                                break;

                            case SKIP_ZEROS:
                                cacheArgs.skipZeros(true);

                                break;

                            case CHECK_CRC:
                                cacheArgs.idleCheckCrc(true);

                                break;

                            case CACHE_FILTER:
                                String filter = nextArg("The cache filter should be specified. The following " +
                                    "values can be used: " + Arrays.toString(CacheFilterEnum.values()) + '.');

                                cacheArgs.setCacheFilterEnum(CacheFilterEnum.valueOf(filter.toUpperCase()));

                                break;

                            case EXCLUDE_CACHES:
                                parseExcludeCacheNames(nextArg("Specify caches, which will be excluded."),
                                    cacheArgs);

                                break;
                        }
                    }
                }
                break;

            case CONTENTION:
                cacheArgs.minQueueSize(Integer.parseInt(nextArg("Min queue size expected")));

                if (hasNextSubArg())
                    cacheArgs.nodeId(UUID.fromString(nextArg("")));

                if (hasNextSubArg())
                    cacheArgs.maxPrint(Integer.parseInt(nextArg("")));
                else
                    cacheArgs.maxPrint(10);

                break;

            case VALIDATE_INDEXES: {
                int argsCnt = 0;

                while (hasNextSubArg() && argsCnt++ < 4) {
                    String nextArg = nextArg("");

                    ValidateIndexesCommandArg arg = CommandArgUtils.of(nextArg, ValidateIndexesCommandArg.class);

                    if (arg == CHECK_FIRST || arg == CHECK_THROUGH) {
                        if (!hasNextSubArg())
                            throw new IllegalArgumentException("Numeric value for '" + nextArg + "' parameter expected.");

                        int numVal;

                        String numStr = nextArg("");

                        try {
                            numVal = Integer.parseInt(numStr);
                        }
                        catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                "Not numeric value was passed for '" + nextArg + "' parameter: " + numStr
                            );
                        }

                        if (numVal <= 0)
                            throw new IllegalArgumentException("Value for '" + nextArg + "' property should be positive.");

                        if (arg == CHECK_FIRST)
                            cacheArgs.checkFirst(numVal);
                        else
                            cacheArgs.checkThrough(numVal);

                        continue;
                    }

                    try {
                        cacheArgs.nodeId(UUID.fromString(nextArg));

                        continue;
                    }
                    catch (IllegalArgumentException ignored) {
                        //No-op.
                    }

                    parseCacheNames(nextArg, cacheArgs);
                }

                break;
            }

            case DISTRIBUTION:
                String nodeIdStr = nextArg("Node id expected or null");
                if (!NULL.equals(nodeIdStr))
                    cacheArgs.nodeId(UUID.fromString(nodeIdStr));

                while (hasNextSubArg()) {
                    String nextArg = nextArg("");

                    DistributionCommandArg arg = CommandArgUtils.of(nextArg, DistributionCommandArg.class);

                    if (arg == USER_ATTRIBUTES) {
                        nextArg = nextArg("User attributes are expected to be separated by commas");

                        Set<String> userAttrs = new HashSet<>();

                        for (String userAttribute : nextArg.split(","))
                            userAttrs.add(userAttribute.trim());

                        cacheArgs.setUserAttributes(userAttrs);

                        nextArg = (hasNextSubArg()) ? nextArg("") : null;

                    }

                    if (nextArg != null)
                        parseCacheNames(nextArg, cacheArgs);
                }

                break;

            case RESET_LOST_PARTITIONS:
                parseCacheNames(nextArg("Cache name expected"), cacheArgs);

                break;

            case LIST:
                cacheArgs.regex(nextArg("Regex is expected"));

                VisorViewCacheCmd cacheCmd = CACHES;

                OutputFormat outputFormat = SINGLE_LINE;

                while (hasNextSubArg()) {
                    String nextArg = nextArg("").toLowerCase();

                    ListCommandArg arg = CommandArgUtils.of(nextArg, ListCommandArg.class);
                    if (arg != null) {
                        switch (arg) {
                            case GROUP:
                                cacheCmd = GROUPS;

                                break;

                            case SEQUENCE:
                                cacheCmd = SEQ;

                                break;

                            case OUTPUT_FORMAT:
                                String tmp2 = nextArg("output format must be defined!").toLowerCase();

                                outputFormat = OutputFormat.fromConsoleName(tmp2);

                                break;

                            case CONFIG:
                                cacheArgs.fullConfig(true);

                                break;
                        }
                    }
                    else
                        cacheArgs.nodeId(UUID.fromString(nextArg));
                }

                cacheArgs.cacheCommand(cacheCmd);
                cacheArgs.outputFormat(outputFormat);

                break;

            case FIND_AND_DELETE_GARBAGE: {
                int argsCnt = 0;

                while (hasNextSubArg() && argsCnt++ < 3) {
                    String nextArg = nextArg("");

                    FindAndDeleteGarbageArg arg = CommandArgUtils.of(nextArg, FindAndDeleteGarbageArg.class);

                    if (arg == FindAndDeleteGarbageArg.DELETE) {
                        cacheArgs.delete(true);

                        continue;
                    }

                    try {
                        cacheArgs.nodeId(UUID.fromString(nextArg));

                        continue;
                    }
                    catch (IllegalArgumentException ignored) {
                        //No-op.
                    }

                    cacheArgs.groups(parseCacheNames(nextArg));
                }

                break;
            }

            default:
                throw new IllegalArgumentException("Unknown --cache subcommand " + cmd);
        }

        if (hasNextSubArg())
            throw new IllegalArgumentException("Unexpected argument of --cache subcommand: " + peekNextArg());

        return cacheArgs;
    }

    /**
     * @return <code>true</code> if there's next argument for subcommand.
     */
    private boolean hasNextSubArg() {
        return hasNextArg() && Command.of(peekNextArg()) == null && !AUX_COMMANDS.contains(peekNextArg());
    }

    /**
     * @param cacheNames Cache names string.
     * @param cacheArgs Cache args.
     */
    private void parseCacheNames(String cacheNames, CacheArguments cacheArgs) {
        cacheArgs.caches(parseCacheNames(cacheNames));
    }

    /**
     * @param cacheNames Cache names arg.
     * @param cacheArgs Cache args.
     */
    private void parseExcludeCacheNames(String cacheNames, CacheArguments cacheArgs) {
        cacheArgs.excludeCaches(parseCacheNames(cacheNames));
    }

    /**
     * @param cacheNames Cache names string.
     */
    private Set<String> parseCacheNames(String cacheNames) {
        String[] cacheNamesArr = cacheNames.split(",");

        Set<String> cacheNamesSet = new HashSet<>();

        for (String cacheName : cacheNamesArr) {
            if (F.isEmpty(cacheName))
                throw new IllegalArgumentException("Non-empty cache names expected.");

            try {
                Pattern.compile(cacheName);
            }
            catch (PatternSyntaxException e) {
                throw new RuntimeException(format("Invalid cache name regexp '%s': %s", cacheName, e.getMessage()));
            }

            cacheNamesSet.add(cacheName.trim());
        }

        return cacheNamesSet;
    }

    /**
     * Get ping param for grid client.
     *
     * @param nextArgErr Argument extraction error message.
     * @param invalidErr Param validation error message.
     */
    private Long getPingParam(String nextArgErr, String invalidErr) {
        String raw = nextArg(nextArgErr);

        try {
            long val = Long.valueOf(raw);

            if (val <= 0)
                throw new IllegalArgumentException(invalidErr + ": " + val);
            else
                return val;
        }
        catch (NumberFormatException ignored) {
            throw new IllegalArgumentException(invalidErr + ": " + raw);
        }
    }

    /**
     * @return Transaction arguments.
     */
    private VisorTxTaskArg parseTransactionArguments() {
        VisorTxProjection proj = null;

        Integer limit = null;

        VisorTxSortOrder sortOrder = null;

        Long duration = null;

        Integer size = null;

        String lbRegex = null;

        List<String> consistentIds = null;

        VisorTxOperation op = VisorTxOperation.LIST;

        String xid = null;

        boolean end = false;

        TxVerboseId txVerboseId = null;

        do {
            String str = peekNextArg();

            if (str == null)
                break;

            switch (str) {
                case TX_LIMIT:
                    nextArg("");

                    limit = (int)nextLongArg(TX_LIMIT);

                    break;

                case TX_ORDER:
                    nextArg("");

                    sortOrder = VisorTxSortOrder.valueOf(nextArg(TX_ORDER).toUpperCase());

                    break;

                case TX_SERVERS:
                    nextArg("");

                    proj = VisorTxProjection.SERVER;

                    break;

                case TX_CLIENTS:
                    nextArg("");

                    proj = VisorTxProjection.CLIENT;

                    break;

                case TX_NODES:
                    nextArg("");

                    consistentIds = getConsistentIds(nextArg(TX_NODES));

                    break;

                case TX_DURATION:
                    nextArg("");

                    duration = nextLongArg(TX_DURATION) * 1000L;

                    break;

                case TX_SIZE:
                    nextArg("");

                    size = (int)nextLongArg(TX_SIZE);

                    break;

                case TX_LABEL:
                    nextArg("");

                    lbRegex = nextArg(TX_LABEL);

                    try {
                        Pattern.compile(lbRegex);
                    }
                    catch (PatternSyntaxException ignored) {
                        throw new IllegalArgumentException("Illegal regex syntax");
                    }

                    break;

                case TX_XID:
                    nextArg("");

                    xid = nextArg(TX_XID);

                    break;

                case TX_KILL:
                    nextArg("");

                    op = VisorTxOperation.KILL;

                    break;

                case TX_INFO:
                    nextArg("");

                    op = VisorTxOperation.INFO;

                    txVerboseId = TxVerboseId.fromString(nextArg(TX_INFO));

                    break;

                default:
                    end = true;
            }
        }
        while (!end);

        if (proj != null && consistentIds != null)
            throw new IllegalArgumentException("Projection can't be used together with list of consistent ids.");

        return new VisorTxTaskArg(
            op, limit, duration, size, null, proj, consistentIds, xid, lbRegex, sortOrder, txVerboseId);
    }

    /**
     * @return Numeric value.
     */
    private long nextLongArg(String lb) {
        String str = nextArg("Expecting " + lb);

        try {
            long val = Long.parseLong(str);

            if (val < 0)
                throw new IllegalArgumentException("Invalid value for " + lb + ": " + val);

            return val;
        }
        catch (NumberFormatException ignored) {
            throw new IllegalArgumentException("Invalid value for " + lb + ": " + str);
        }
    }

    /**
     * Requests password from console with message.
     *
     * @param msg Message.
     * @return Password.
     */
    private char[] requestPasswordFromConsole(String msg) {
        if (console == null)
            throw new UnsupportedOperationException("Failed to securely read password (console is unavailable): " + msg);
        else
            return console.readPassword(msg);
    }

    /**
     * Requests user data from console with message.
     *
     * @param msg Message.
     * @return Input user data.
     */
    private String requestDataFromConsole(String msg) {
        if (console != null)
            return console.readLine(msg);
        else {
            Scanner scanner = new Scanner(System.in);

            log(msg);

            return scanner.nextLine();
        }
    }

    /**
     * Check if raw arg is command or option.
     *
     * @return {@code true} If raw arg is command, overwise {@code false}.
     */
    private boolean isCommandOrOption(String raw) {
        return raw != null && raw.contains("--");
    }

    /**
     * Maps VisorCacheConfiguration to key-value pairs.
     *
     * @param cfg Visor cache configuration.
     * @return map of key-value pairs.
     */
    private Map<String, Object> mapToPairs(VisorCacheConfiguration cfg) {
        Map<String, Object> params = new LinkedHashMap<>();

        VisorCacheAffinityConfiguration affinityCfg = cfg.getAffinityConfiguration();
        VisorCacheNearConfiguration nearCfg = cfg.getNearConfiguration();
        VisorCacheRebalanceConfiguration rebalanceCfg = cfg.getRebalanceConfiguration();
        VisorCacheEvictionConfiguration evictCfg = cfg.getEvictionConfiguration();
        VisorCacheStoreConfiguration storeCfg = cfg.getStoreConfiguration();
        VisorQueryConfiguration qryCfg = cfg.getQueryConfiguration();

        params.put("Name", cfg.getName());
        params.put("Group", cfg.getGroupName());
        params.put("Dynamic Deployment ID", cfg.getDynamicDeploymentId());
        params.put("System", cfg.isSystem());

        params.put("Mode", cfg.getMode());
        params.put("Atomicity Mode", cfg.getAtomicityMode());
        params.put("Statistic Enabled", cfg.isStatisticsEnabled());
        params.put("Management Enabled", cfg.isManagementEnabled());

        params.put("On-heap cache enabled", cfg.isOnheapCacheEnabled());
        params.put("Partition Loss Policy", cfg.getPartitionLossPolicy());
        params.put("Query Parallelism", cfg.getQueryParallelism());
        params.put("Copy On Read", cfg.isCopyOnRead());
        params.put("Listener Configurations", cfg.getListenerConfigurations());
        params.put("Load Previous Value", cfg.isLoadPreviousValue());
        params.put("Memory Policy Name", cfg.getMemoryPolicyName());
        params.put("Node Filter", cfg.getNodeFilter());
        params.put("Read From Backup", cfg.isReadFromBackup());
        params.put("Topology Validator", cfg.getTopologyValidator());

        params.put("Time To Live Eager Flag", cfg.isEagerTtl());

        params.put("Write Synchronization Mode", cfg.getWriteSynchronizationMode());
        params.put("Invalidate", cfg.isInvalidate());

        params.put("Affinity Function", affinityCfg.getFunction());
        params.put("Affinity Backups", affinityCfg.getPartitionedBackups());
        params.put("Affinity Partitions", affinityCfg.getPartitions());
        params.put("Affinity Exclude Neighbors", affinityCfg.isExcludeNeighbors());
        params.put("Affinity Mapper", affinityCfg.getMapper());

        params.put("Rebalance Mode", rebalanceCfg.getMode());
        params.put("Rebalance Batch Size", rebalanceCfg.getBatchSize());
        params.put("Rebalance Timeout", rebalanceCfg.getTimeout());
        params.put("Rebalance Delay", rebalanceCfg.getPartitionedDelay());
        params.put("Time Between Rebalance Messages", rebalanceCfg.getThrottle());
        params.put("Rebalance Batches Count", rebalanceCfg.getBatchesPrefetchCnt());
        params.put("Rebalance Cache Order", rebalanceCfg.getRebalanceOrder());

        params.put("Eviction Policy Enabled", (evictCfg.getPolicy() != null));
        params.put("Eviction Policy Factory", evictCfg.getPolicy());
        params.put("Eviction Policy Max Size", evictCfg.getPolicyMaxSize());
        params.put("Eviction Filter", evictCfg.getFilter());

        params.put("Near Cache Enabled", nearCfg.isNearEnabled());
        params.put("Near Start Size", nearCfg.getNearStartSize());
        params.put("Near Eviction Policy Factory", nearCfg.getNearEvictPolicy());
        params.put("Near Eviction Policy Max Size", nearCfg.getNearEvictMaxSize());

        params.put("Default Lock Timeout", cfg.getDefaultLockTimeout());
        params.put("Query Entities", cfg.getQueryEntities());
        params.put("Cache Interceptor", cfg.getInterceptor());

        params.put("Store Enabled", storeCfg.isEnabled());
        params.put("Store Class", storeCfg.getStore());
        params.put("Store Factory Class", storeCfg.getStoreFactory());
        params.put("Store Keep Binary", storeCfg.isStoreKeepBinary());
        params.put("Store Read Through", storeCfg.isReadThrough());
        params.put("Store Write Through", storeCfg.isWriteThrough());
        params.put("Store Write Coalescing", storeCfg.getWriteBehindCoalescing());

        params.put("Write-Behind Enabled", storeCfg.isWriteBehindEnabled());
        params.put("Write-Behind Flush Size", storeCfg.getFlushSize());
        params.put("Write-Behind Frequency", storeCfg.getFlushFrequency());
        params.put("Write-Behind Flush Threads Count", storeCfg.getFlushThreadCount());
        params.put("Write-Behind Batch Size", storeCfg.getBatchSize());

        params.put("Concurrent Asynchronous Operations Number", cfg.getMaxConcurrentAsyncOperations());

        params.put("Loader Factory Class Name", cfg.getLoaderFactory());
        params.put("Writer Factory Class Name", cfg.getWriterFactory());
        params.put("Expiry Policy Factory Class Name", cfg.getExpiryPolicyFactory());

        params.put("Query Execution Time Threshold", qryCfg.getLongQueryWarningTimeout());
        params.put("Query Escaped Names", qryCfg.isSqlEscapeAll());
        params.put("Query SQL Schema", qryCfg.getSqlSchema());
        params.put("Query SQL functions", qryCfg.getSqlFunctionClasses());
        params.put("Query Indexed Types", qryCfg.getIndexedTypes());
        params.put("Maximum payload size for offheap indexes", cfg.getSqlIndexMaxInlineSize());
        params.put("Query Metrics History Size", cfg.getQueryDetailMetricsSize());

        return params;
    }

    /**
     * Split string into items.
     *
     * @param s String to process.
     * @param delim Delimiter.
     * @return List with items.
     */
    private List<String> split(String s, String delim) {
        if (F.isEmpty(s))
            return Collections.emptyList();

        return Arrays.stream(s.split(delim))
            .map(String::trim)
            .filter(item -> !item.isEmpty())
            .collect(Collectors.toList());
    }

    /**
     * @return Transaction command options.
     */
    private String[] getTxOptions() {
        List<String> list = new ArrayList<>();

        list.add(op(TX_XID, "XID"));
        list.add(op(TX_DURATION, "SECONDS"));
        list.add(op(TX_SIZE, "SIZE"));
        list.add(op(TX_LABEL, "PATTERN_REGEX"));
        list.add(op(or(TX_SERVERS, TX_CLIENTS)));
        list.add(op(TX_NODES, "consistentId1[,consistentId2,....,consistentIdN]"));
        list.add(op(TX_LIMIT, "NUMBER"));
        list.add(op(TX_ORDER, or(VisorTxSortOrder.values())));
        list.add(op(TX_KILL));
        list.add(op(TX_INFO));
        list.add(op(CMD_AUTO_CONFIRMATION));

        return list.toArray(new String[list.size()]);
    }

    /** */
    private void printHelp() {
        final String constistIds = "consistentId1[,consistentId2,....,consistentIdN]";

        log("Control.sh is used to execute admin commands on cluster or get common cluster info. The command has the following syntax:");
        nl();

        log(i(j(" ", UTILITY_NAME_WITH_COMMON_OPTIONS, op("command"), "<command_parameters>")));
        nl();
        nl();

        log("This utility can do the following commands:");

        usage(i("Activate cluster:"), ACTIVATE);
        usage(i("Deactivate cluster:"), DEACTIVATE, op(CMD_AUTO_CONFIRMATION));
        usage(i("Print current cluster state:"), STATE);
        usage(i("Print cluster baseline topology:"), BASELINE);
        usage(i("Add nodes into baseline topology:"), BASELINE, BaselineCommand.ADD.text(), constistIds, op(CMD_AUTO_CONFIRMATION));
        usage(i("Remove nodes from baseline topology:"), BASELINE, BaselineCommand.REMOVE.text(), constistIds, op(CMD_AUTO_CONFIRMATION));
        usage(i("Set baseline topology:"), BASELINE, BaselineCommand.SET.text(), constistIds, op(CMD_AUTO_CONFIRMATION));
        usage(i("Set baseline topology based on version:"), BASELINE, BaselineCommand.VERSION.text() + " topologyVersion", op(CMD_AUTO_CONFIRMATION));
        usage(i("Set baseline autoadjustment settings:"), BASELINE, BaselineCommand.AUTO_ADJUST.text(), "disable|enable timeout <timeoutValue>", op(CMD_AUTO_CONFIRMATION));
        usage(i("List or kill transactions:"), TX, getTxOptions());
        usage(i("Print detailed information (topology and key lock ownership) about specific transaction:"),
            TX, TX_INFO, or("<TX identifier as GridCacheVersion [topVer=..., order=..., nodeOrder=...] " +
                "(can be found in logs)>", "<TX identifier as UUID (can be retrieved via --tx command)>"));

        if (enableExperimental) {
            usage(i("Print absolute paths of unused archived wal segments on each node:"), WAL, WAL_PRINT, "[consistentId1,consistentId2,....,consistentIdN]");
            usage(i("Delete unused archived wal segments on each node:"), WAL, WAL_DELETE, "[consistentId1,consistentId2,....,consistentIdN]", op(CMD_AUTO_CONFIRMATION));
        }

        log(i("View caches information in a cluster. For more details type:"));
        log(i(j(" ", UTILITY_NAME, CACHE, HELP), 2));
        nl();

        log("By default commands affecting the cluster require interactive confirmation.");
        log("Use " + CMD_AUTO_CONFIRMATION + " option to disable it.");
        nl();

        log("Default values:");
        log(i("HOST_OR_IP=" + DFLT_HOST, 2));
        log(i("PORT=" + DFLT_PORT, 2));
        log(i("PING_INTERVAL=" + DFLT_PING_INTERVAL, 2));
        log(i("PING_TIMEOUT=" + DFLT_PING_TIMEOUT, 2));
        log(i("SSL_PROTOCOL=" + SslContextFactory.DFLT_SSL_PROTOCOL, 2));
        log(i("SSL_KEY_ALGORITHM=" + SslContextFactory.DFLT_KEY_ALGORITHM, 2));
        log(i("KEYSTORE_TYPE=" + SslContextFactory.DFLT_STORE_TYPE, 2));
        log(i("TRUSTSTORE_TYPE=" + SslContextFactory.DFLT_STORE_TYPE, 2));

        nl();

        log("Exit codes:");
        log(i(EXIT_CODE_OK + " - successful execution.", 2));
        log(i(EXIT_CODE_INVALID_ARGUMENTS + " - invalid arguments.", 2));
        log(i(EXIT_CODE_CONNECTION_FAILED + " - connection failed.", 2));
        log(i(ERR_AUTHENTICATION_FAILED + " - authentication failed.", 2));
        log(i(EXIT_CODE_UNEXPECTED_ERROR + " - unexpected error.", 2));
    }

    /**
     * Parse and execute command.
     *
     * @param rawArgs Arguments to parse and execute.
     * @return Exit code.
     */
    public int execute(List<String> rawArgs) {
        log("Control utility [ver. " + ACK_VER_STR + "]");
        log(COPYRIGHT);
        log("User: " + System.getProperty("user.name"));
        log("Time: " + LocalDateTime.now());
        log(DELIM);

        try {
            if (F.isEmpty(rawArgs) || (rawArgs.size() == 1 && CMD_HELP.equalsIgnoreCase(rawArgs.get(0)))) {
                printHelp();

                return EXIT_CODE_OK;
            }

            Arguments args = parseAndValidate(rawArgs);

            if (args.command() == CACHE && args.cacheArgs().command() == HELP) {
                printCacheHelp();

                return EXIT_CODE_OK;
            }

            if (!args.autoConfirmation() && !confirm(args)) {
                log("Operation cancelled.");

                return EXIT_CODE_OK;
            }

            clientCfg = new GridClientConfiguration();

            clientCfg.setPingInterval(args.pingInterval());

            clientCfg.setPingTimeout(args.pingTimeout());

            clientCfg.setServers(Collections.singletonList(args.host() + ":" + args.port()));

            boolean tryConnectAgain = true;

            boolean suppliedAuth = !F.isEmpty(args.getUserName()) && !F.isEmpty(args.getPassword());

            int tryConnectMaxCount = 3;

            while (tryConnectAgain) {
                tryConnectAgain = false;

                if (!F.isEmpty(args.getUserName())) {
                    SecurityCredentialsProvider securityCredential = clientCfg.getSecurityCredentialsProvider();

                    if (securityCredential == null) {
                        securityCredential = new SecurityCredentialsBasicProvider(
                            new SecurityCredentials(args.getUserName(), args.getPassword()));

                        clientCfg.setSecurityCredentialsProvider(securityCredential);
                    }
                    final SecurityCredentials credential = securityCredential.credentials();
                    credential.setLogin(args.getUserName());
                    credential.setPassword(args.getPassword());
                }

                if (!F.isEmpty(args.sslKeyStorePath())) {
                    GridSslBasicContextFactory factory = new GridSslBasicContextFactory();

                    List<String> sslProtocols = split(args.sslProtocol(), ",");

                    String sslProtocol = F.isEmpty(sslProtocols) ? DFLT_SSL_PROTOCOL : sslProtocols.get(0);

                    factory.setProtocol(sslProtocol);
                    factory.setKeyAlgorithm(args.sslKeyAlgorithm());

                    if (sslProtocols.size() > 1)
                        factory.setProtocols(sslProtocols);

                    factory.setCipherSuites(split(args.getSslCipherSuites(), ","));

                    factory.setKeyStoreFilePath(args.sslKeyStorePath());

                    if (args.sslKeyStorePassword() != null)
                        factory.setKeyStorePassword(args.sslKeyStorePassword());
                    else
                        factory.setKeyStorePassword(requestPasswordFromConsole("SSL keystore password: "));

                    factory.setKeyStoreType(args.sslKeyStoreType());

                    if (F.isEmpty(args.sslTrustStorePath()))
                        factory.setTrustManagers(GridSslBasicContextFactory.getDisabledTrustManager());
                    else {
                        factory.setTrustStoreFilePath(args.sslTrustStorePath());

                        if (args.sslTrustStorePassword() != null)
                            factory.setTrustStorePassword(args.sslTrustStorePassword());
                        else
                            factory.setTrustStorePassword(requestPasswordFromConsole("SSL truststore password: "));

                        factory.setTrustStoreType(args.sslTrustStoreType());
                    }

                    clientCfg.setSslContextFactory(factory);
                }

                try (GridClient client = GridClientFactory.start(clientCfg)) {
                    // If connection is unsuccessful, fail before doing any operations:
                    if (!client.connected())
                        client.throwLastError();

                    switch (args.command()) {
                        case ACTIVATE:
                            activate(client);

                            break;

                        case DEACTIVATE:
                            deactivate(client);

                            break;

                        case STATE:
                            state(client);

                            break;

                        case BASELINE:
                            baseline(client, args.baselineArguments());

                            break;

                        case TX:
                            transactions(client, args.transactionArguments());

                            break;

                        case CACHE:
                            lastOperationRes = cache(client, args.cacheArgs());

                            break;

                        case WAL:
                            wal(client, args.walAction(), args.walArguments());

                            break;
                    }
                }
                catch (Throwable e) {
                    if (tryConnectMaxCount > 0 && isAuthError(e)) {
                        log(suppliedAuth ?
                            "Authentication error, please try again." : "This cluster requires authentication.");

                        if (F.isEmpty(args.getUserName()))
                            args.setUserName(requestDataFromConsole("user: "));

                        args.setPassword(new String(requestPasswordFromConsole("password: ")));

                        suppliedAuth = true;

                        tryConnectAgain = true;

                        tryConnectMaxCount--;
                    }
                    else {
                        if (tryConnectMaxCount == 0)
                            throw new GridClientAuthenticationException("Authentication error, maximum number of " +
                                "retries exceeded");

                        throw e;
                    }
                }
            }
            return EXIT_CODE_OK;
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

        System.exit(hnd.execute(Arrays.asList(args)));
    }

    /**
     * Used for tests.
     *
     * @return Last operation result;
     */
    public <T> T getLastOperationResult() {
        return (T)lastOperationRes;
    }
}

