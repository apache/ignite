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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.baseline.AutoAdjustCommandArg;
import org.apache.ignite.internal.commandline.baseline.BaselineArguments;
import org.apache.ignite.internal.commandline.baseline.BaselineCommand;
import org.apache.ignite.internal.commandline.cache.CacheArguments;
import org.apache.ignite.internal.commandline.cache.CacheCommandList;
import org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.FindAndRemoveGarbageArg;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ListCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;
import org.apache.ignite.internal.visor.verify.VisorViewCacheCmd;
import org.apache.ignite.ssl.SslContextFactory;

import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_INTERVAL;
import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.Commands.BASELINE;
import static org.apache.ignite.internal.commandline.Commands.CACHE;
import static org.apache.ignite.internal.commandline.Commands.TX;
import static org.apache.ignite.internal.commandline.Commands.WAL;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_KEYSTORE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_KEYSTORE_TYPE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_PASSWORD;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_PING_INTERVAL;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_SSL_CIPHER_SUITES;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_SSL_KEY_ALGORITHM;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_SSL_PROTOCOL;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_TRUSTSTORE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_TRUSTSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_TRUSTSTORE_TYPE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_USER;
import static org.apache.ignite.internal.commandline.CommandHandler.NULL;
import static org.apache.ignite.internal.commandline.CommandHandler.ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG;
import static org.apache.ignite.internal.commandline.OutputFormat.SINGLE_LINE;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_DELETE;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_PRINT;
import static org.apache.ignite.internal.commandline.baseline.BaselineCommand.of;
import static org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg.USER_ATTRIBUTES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.CACHES;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.GROUPS;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.SEQ;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;

public class CommandArgParser {
    private final Set<String> auxCommands;
    private final boolean enableExperimental;
    private final CommandLogger logger;

    public CommandArgParser(Set<String> commands, boolean enableExperimental, CommandLogger logger) {
        auxCommands = commands;
        this.enableExperimental = enableExperimental;
        this.logger = logger;
    }

    /**
     * Parses and validates arguments.
     *
     * @param argIter Iterator of arguments.
     * @return Arguments bean.
     * @throws IllegalArgumentException In case arguments aren't valid.
     */
    Arguments parseAndValidate(CommandArgIterator argIter) {
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

        List<Commands> commands = new ArrayList<>();

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

        while (argIter.hasNextArg()) {
            String str = argIter.nextArg("").toLowerCase();

            Commands cmd = Commands.of(str);

            if (cmd != null) {
                switch (cmd) {
                    case ACTIVATE:
                    case DEACTIVATE:
                    case STATE:
                        commands.add(cmd);

                        break;

                    case TX:
                        commands.add(TX);

                        txArgs = parseTransactionArguments(argIter);

                        break;

                    case BASELINE:
                        commands.add(BASELINE);

                        baselineArgs = parseAndValidateBaselineArgs(argIter);

                        break;

                    case CACHE:
                        commands.add(CACHE);

                        cacheArgs = parseAndValidateCacheArgs(argIter);

                        break;

                    case WAL:
                        if (!enableExperimental)
                            throw new IllegalArgumentException("Experimental command is disabled.");

                        commands.add(WAL);

                        str = argIter.nextArg("Expected arguments for " + WAL.text());

                        walAct = str.toLowerCase();

                        if (WAL_PRINT.equals(walAct) || WAL_DELETE.equals(walAct))
                            walArgs = (str = argIter.peekNextArg()) != null && !isCommandOrOption(str)
                                ? argIter.nextArg("Unexpected argument for " + WAL.text() + ": " + walAct)
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
                        host = argIter.nextArg("Expected host name");

                        break;

                    case CMD_PORT:
                        port = argIter.nextArg("Expected port number");

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
                        pingInterval = getPingParam(argIter, "Expected ping interval", "Invalid value for ping interval");

                        break;

                    case CMD_PING_TIMEOUT:
                        pingTimeout = getPingParam(argIter, "Expected ping timeout", "Invalid value for ping timeout");

                        break;

                    case CMD_USER:
                        user = argIter.nextArg("Expected user name");

                        break;

                    case CMD_PASSWORD:
                        pwd = argIter.nextArg("Expected password");

                        logger.log(String.format(pwdArgWarnFmt, CMD_PASSWORD, CMD_PASSWORD));

                        break;

                    case CMD_SSL_PROTOCOL:
                        sslProtocol = argIter.nextArg("Expected SSL protocol");

                        break;

                    case CMD_SSL_CIPHER_SUITES:
                        sslCipherSuites = argIter.nextArg("Expected SSL cipher suites");

                        break;

                    case CMD_SSL_KEY_ALGORITHM:
                        sslKeyAlgorithm = argIter.nextArg("Expected SSL key algorithm");

                        break;

                    case CMD_KEYSTORE:
                        sslKeyStorePath = argIter.nextArg("Expected SSL key store path");

                        break;

                    case CMD_KEYSTORE_PASSWORD:
                        sslKeyStorePassword = argIter.nextArg("Expected SSL key store password").toCharArray();

                        logger.log(String.format(pwdArgWarnFmt, CMD_KEYSTORE_PASSWORD, CMD_KEYSTORE_PASSWORD));

                        break;

                    case CMD_KEYSTORE_TYPE:
                        sslKeyStoreType = argIter.nextArg("Expected SSL key store type");

                        break;

                    case CMD_TRUSTSTORE:
                        sslTrustStorePath = argIter.nextArg("Expected SSL trust store path");

                        break;

                    case CMD_TRUSTSTORE_PASSWORD:
                        sslTrustStorePassword = argIter.nextArg("Expected SSL trust store password").toCharArray();

                        logger.log(String.format(pwdArgWarnFmt, CMD_TRUSTSTORE_PASSWORD, CMD_TRUSTSTORE_PASSWORD));

                        break;

                    case CMD_TRUSTSTORE_TYPE:
                        sslTrustStoreType = argIter.nextArg("Expected SSL trust store type");

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

        Commands cmd = commands.get(0);

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
     * Check if raw arg is command or option.
     *
     * @return {@code true} If raw arg is command, overwise {@code false}.
     */
    private static boolean isCommandOrOption(String raw) {
        return raw != null && raw.contains("--");
    }

    /**
     * Parses and validates baseline arguments.
     *
     * @return --baseline subcommand arguments in case validation is successful.
     * @param argIter
     */
    private BaselineArguments parseAndValidateBaselineArgs(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg(auxCommands))
            return new BaselineArguments.Builder(BaselineCommand.COLLECT).build();

        BaselineCommand cmd = of(argIter.nextArg("Expected baseline action"));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct baseline action");

        BaselineArguments.Builder baselineArgs = new BaselineArguments.Builder(cmd);

        switch (cmd) {
            case ADD:
            case REMOVE:
            case SET:
                return baselineArgs
                    .withConsistentIds(getConsistentIds(argIter.nextArg("Expected list of consistent ids")))
                    .build();
            case VERSION:
                return baselineArgs
                    .withTopVer(nextLongArg(argIter, "topology version"))
                    .build();
            case AUTO_ADJUST:
                do {
                    AutoAdjustCommandArg autoAdjustArg = CommandArgUtils.of(
                        argIter.nextArg("Expected one of auto-adjust arguments"), AutoAdjustCommandArg.class
                    );

                    if (autoAdjustArg == null)
                        throw new IllegalArgumentException("Expected one of auto-adjust arguments");

                    if (autoAdjustArg == AutoAdjustCommandArg.ENABLE || autoAdjustArg == AutoAdjustCommandArg.DISABLE)
                        baselineArgs.withEnable(autoAdjustArg == AutoAdjustCommandArg.ENABLE);

                    if (autoAdjustArg == AutoAdjustCommandArg.TIMEOUT)
                        baselineArgs.withSoftBaselineTimeout(nextLongArg(argIter, "soft timeout"));
                }
                while (argIter.hasNextSubArg(auxCommands));

                return baselineArgs.build();
        }

        return baselineArgs.build();
    }

    /**
     * Parses and validates cache arguments.
     *
     * @return --cache subcommand arguments in case validation is successful.
     * @param argIter Argument iterator.
     */
    private CacheArguments parseAndValidateCacheArgs(
        CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg(auxCommands)) {
            throw new IllegalArgumentException("Arguments are expected for --cache subcommand, " +
                "run --cache help for more info.");
        }

        CacheArguments cacheArgs = new CacheArguments();

        String str = argIter.nextArg("").toLowerCase();

        CacheCommandList cmd = CacheCommandList.of(str);

        if (cmd == null)
            cmd = CacheCommandList.HELP;

        cacheArgs.command(cmd);

        switch (cmd) {
            case HELP:
                break;

            case IDLE_VERIFY:
                int idleVerifyArgsCnt = 3;

                while (argIter.hasNextSubArg(auxCommands) && idleVerifyArgsCnt-- > 0) {
                    String nextArg = argIter.nextArg("");

                    IdleVerifyCommandArg arg = CommandArgUtils.of(nextArg, IdleVerifyCommandArg.class);

                    if (arg == null) {
                        if (cacheArgs.excludeCaches() != null || cacheArgs.getCacheFilterEnum() != CacheFilterEnum.ALL)
                            throw new IllegalArgumentException(ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG);

                        parseCacheNames(nextArg, cacheArgs);
                    }
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
                                if (cacheArgs.caches() != null || cacheArgs.excludeCaches() != null)
                                    throw new IllegalArgumentException(ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG);

                                String filter = argIter.nextArg("The cache filter should be specified. The following " +
                                    "values can be used: " + Arrays.toString(CacheFilterEnum.values()) + '.');

                                cacheArgs.setCacheFilterEnum(CacheFilterEnum.valueOf(filter.toUpperCase()));

                                break;

                            case EXCLUDE_CACHES:
                                if (cacheArgs.caches() != null || cacheArgs.getCacheFilterEnum() != CacheFilterEnum.ALL)
                                    throw new IllegalArgumentException(ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG);

                                parseExcludeCacheNames(argIter.nextArg("Specify caches, which will be excluded."),
                                    cacheArgs);

                                break;
                        }
                    }
                }
                break;

            case CONTENTION:
                cacheArgs.minQueueSize(Integer.parseInt(argIter.nextArg("Min queue size expected")));

                if (argIter.hasNextSubArg(auxCommands))
                    cacheArgs.nodeId(UUID.fromString(argIter.nextArg("")));

                if (argIter.hasNextSubArg(auxCommands))
                    cacheArgs.maxPrint(Integer.parseInt(argIter.nextArg("")));
                else
                    cacheArgs.maxPrint(10);

                break;

            case VALIDATE_INDEXES: {
                int argsCnt = 0;

                while (argIter.hasNextSubArg(auxCommands) && argsCnt++ < 4) {
                    String nextArg = argIter.nextArg("");

                    ValidateIndexesCommandArg arg = CommandArgUtils.of(nextArg, ValidateIndexesCommandArg.class);

                    if (arg == CHECK_FIRST || arg == CHECK_THROUGH) {
                        if (!argIter.hasNextSubArg(auxCommands))
                            throw new IllegalArgumentException("Numeric value for '" + nextArg + "' parameter expected.");

                        int numVal;

                        String numStr = argIter.nextArg("");

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

            case FIND_AND_REMOVE_GARBAGE: {
                int argsCnt = 0;

                while (argIter.hasNextSubArg(auxCommands) && argsCnt++ < 3) {
                    String nextArg = argIter.nextArg("");

                    FindAndRemoveGarbageArg arg = CommandArgUtils.of(nextArg, FindAndRemoveGarbageArg.class);

                    if (arg == FindAndRemoveGarbageArg.DELETE) {
                        cacheArgs.delete(true);
                    }

                    try {
                        cacheArgs.nodeId(UUID.fromString(nextArg));

                        continue;
                    }
                    catch (IllegalArgumentException ignored) {
                        //No-op.
                    }

                    parseGroupNames(nextArg, cacheArgs);
                }

                break;
            }

            case DISTRIBUTION:
                String nodeIdStr = argIter.nextArg("Node id expected or null");
                if (!NULL.equals(nodeIdStr))
                    cacheArgs.nodeId(UUID.fromString(nodeIdStr));

                while (argIter.hasNextSubArg(auxCommands)) {
                    String nextArg = argIter.nextArg("");

                    DistributionCommandArg arg = CommandArgUtils.of(nextArg, DistributionCommandArg.class);

                    if (arg == USER_ATTRIBUTES) {
                        nextArg = argIter.nextArg("User attributes are expected to be separated by commas");

                        Set<String> userAttrs = new HashSet<>();

                        for (String userAttribute : nextArg.split(","))
                            userAttrs.add(userAttribute.trim());

                        cacheArgs.setUserAttributes(userAttrs);

                        nextArg = (argIter.hasNextSubArg(auxCommands)) ? argIter.nextArg("") : null;

                    }

                    if (nextArg != null)
                        parseCacheNames(nextArg, cacheArgs);
                }

                break;

            case RESET_LOST_PARTITIONS:
                parseCacheNames(argIter.nextArg("Cache name expected"), cacheArgs);

                break;

            case LIST:
                cacheArgs.regex(argIter.nextArg("Regex is expected"));

                VisorViewCacheCmd cacheCmd = CACHES;

                OutputFormat outputFormat = SINGLE_LINE;

                while (argIter.hasNextSubArg(auxCommands)) {
                    String nextArg = argIter.nextArg("").toLowerCase();

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
                                String tmp2 = argIter.nextArg("output format must be defined!").toLowerCase();

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

            default:
                throw new IllegalArgumentException("Unknown --cache subcommand " + cmd);
        }

        if (argIter.hasNextSubArg(auxCommands))
            throw new IllegalArgumentException("Unexpected argument of --cache subcommand: " + argIter.peekNextArg());

        return cacheArgs;
    }

    /**
     * @param cacheNames Cache names string.
     * @param cacheArgs Cache args.
     */
    private void parseCacheNames(String cacheNames, CacheArguments cacheArgs) {
        cacheArgs.caches(parseNames(cacheNames));
    }

    /**
     * @param groupNames Cache group names string.
     * @param cacheArgs Cache args.
     */
    private void parseGroupNames(String groupNames, CacheArguments cacheArgs) {
        cacheArgs.groups(parseNames(groupNames));
    }

    /**
     * @param cacheNames Cache names arg.
     * @param cacheArgs Cache args.
     */
    private void parseExcludeCacheNames(String cacheNames, CacheArguments cacheArgs) {
        cacheArgs.excludeCaches(parseNames(cacheNames));
    }

    /**
     * @param names Cache names string.
     */
    private Set<String> parseNames(String names) {
        String[] namesArr = names.split(",");

        Set<String> namesSet = new HashSet<>();

        for (String name : namesArr) {
            if (F.isEmpty(name))
                throw new IllegalArgumentException("Non-empty names expected.");

            namesSet.add(name.trim());
        }

        return namesSet;
    }

    /**
     * Get ping param for grid client.
     *
     * @param argIter Argument iterator.
     * @param nextArgErr Argument extraction error message.
     * @param invalidErr Param validation error message.
     */
    private Long getPingParam(CommandArgIterator argIter, String nextArgErr, String invalidErr) {
        String raw = argIter.nextArg(nextArgErr);

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
     * @param argIter Argument iterator.
     */
    private VisorTxTaskArg parseTransactionArguments(CommandArgIterator argIter) {
        VisorTxProjection proj = null;

        Integer limit = null;

        VisorTxSortOrder sortOrder = null;

        Long duration = null;

        Integer size = null;

        String lbRegex = null;

        List<String> consistentIds = null;

        VisorTxOperation op = VisorTxOperation.LIST;

        String xid = null;

        while (true) {
            String str = argIter.peekNextArg();

            if (str == null)
                break;

            TxCommandArg arg = CommandArgUtils.of(str, TxCommandArg.class);

            if (arg == null)
                break;

            switch (arg) {
                case TX_LIMIT:
                    argIter.nextArg("");

                    limit = (int)nextLongArg(argIter, TxCommandArg.TX_LIMIT.toString());

                    break;

                case TX_ORDER:
                    argIter.nextArg("");

                    sortOrder = VisorTxSortOrder.valueOf(argIter.nextArg(TxCommandArg.TX_ORDER.toString()).toUpperCase());

                    break;

                case TX_SERVERS:
                    argIter.nextArg("");

                    proj = VisorTxProjection.SERVER;
                    break;

                case TX_CLIENTS:
                    argIter.nextArg("");

                    proj = VisorTxProjection.CLIENT;
                    break;

                case TX_NODES:
                    argIter.nextArg("");

                    consistentIds = getConsistentIds(argIter.nextArg(TxCommandArg.TX_NODES.toString()));
                    break;

                case TX_DURATION:
                    argIter.nextArg("");

                    duration = nextLongArg(argIter, TxCommandArg.TX_DURATION.toString()) * 1000L;
                    break;

                case TX_SIZE:
                    argIter.nextArg("");

                    size = (int)nextLongArg(argIter, TxCommandArg.TX_SIZE.toString());
                    break;

                case TX_LABEL:
                    argIter.nextArg("");

                    lbRegex = argIter.nextArg(TxCommandArg.TX_LABEL.toString());

                    try {
                        Pattern.compile(lbRegex);
                    }
                    catch (PatternSyntaxException ignored) {
                        throw new IllegalArgumentException("Illegal regex syntax");
                    }

                    break;

                case TX_XID:
                    argIter.nextArg("");

                    xid = argIter.nextArg(TxCommandArg.TX_XID.toString());
                    break;

                case TX_KILL:
                    argIter.nextArg("");

                    op = VisorTxOperation.KILL;
                    break;

                default:
                    throw new AssertionError();
            }
        }


        if (proj != null && consistentIds != null)
            throw new IllegalArgumentException("Projection can't be used together with list of consistent ids.");

        return new VisorTxTaskArg(op, limit, duration, size, null, proj, consistentIds, xid, lbRegex, sortOrder);
    }

    /**
     * @return Numeric value.
     */
    private long nextLongArg(CommandArgIterator argIter, String lb) {
        String str = argIter.nextArg("Expecting " + lb);

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
}
