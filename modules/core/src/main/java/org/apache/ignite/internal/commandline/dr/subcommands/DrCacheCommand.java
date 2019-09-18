/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.dr.subcommands;

import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.dr.DrSubCommandsList;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.dr.VisorDrCacheTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrCacheTaskResult;

import static org.apache.ignite.internal.commandline.CommandHandler.DELIM;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;

/** */
public class DrCacheCommand extends
    DrAbstractRemoteSubCommand<VisorDrCacheTaskArgs, VisorDrCacheTaskResult, DrCacheCommand.DrCacheArguments>
{
    /** Config parameter. */
    public static final String CONFIG_PARAM = "--config";
    /** Metrics parameter. */
    public static final String METRICS_PARAM = "--metrics";
    /** Cache filter parameter. */
    public static final String CACHE_FILTER_PARAM = "--cache-filter";
    /** Sender group parameter. */
    public static final String SENDER_GROUP_PARAM = "--sender-group";
    /** Action parameter. */
    public static final String ACTION_PARAM = "--action";

    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.gridgain.grid.internal.visor.dr.console.VisorDrCacheTask";
    }

    /** {@inheritDoc} */
    @Override public DrCacheArguments parseArguments0(CommandArgIterator argIter) {
        String regex = argIter.nextArg("Cache name regex expected.");

        if (CommandArgIterator.isCommandOrOption(regex))
            throw new IllegalArgumentException("Cache name regex expected.");

        try {
            Pattern.compile(regex);
        }
        catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Cache name regex is not valid.", e);
        }

        boolean cfg = false;
        boolean metrics = false;
        CacheFilter cacheFilter = CacheFilter.ALL;
        SenderGroup sndGrp = SenderGroup.ALL;
        String sndGrpName = null;
        Action act = null;

        String nextArg;

        //noinspection LabeledStatement
        args_loop: while ((nextArg = argIter.peekNextArg()) != null) {
            switch (nextArg.toLowerCase(Locale.ENGLISH)) {
                case CONFIG_PARAM:
                    argIter.nextArg(null);
                    cfg = true;

                    break;

                case METRICS_PARAM:
                    argIter.nextArg(null);
                    metrics = true;

                    break;

                case CACHE_FILTER_PARAM: {
                    argIter.nextArg(null);

                    String errorMsg = "--cache-filter parameter value required.";

                    String cacheFilterStr = argIter.nextArg(errorMsg);
                    cacheFilter = CacheFilter.valueOf(cacheFilterStr.toUpperCase(Locale.ENGLISH));

                    if (cacheFilter == null)
                        throw new IllegalArgumentException(errorMsg);

                    break;
                }

                case SENDER_GROUP_PARAM: {
                    argIter.nextArg(null);

                    String arg = argIter.nextArg("--sender-group parameter value required.");

                    sndGrp = SenderGroup.parse(arg);

                    if (sndGrp == null)
                        sndGrpName = arg;

                    break;
                }

                case ACTION_PARAM: {
                    argIter.nextArg(null);

                    String errorMsg = "--action parameter value required.";

                    act = Action.parse(argIter.nextArg(errorMsg));

                    if (act == null)
                        throw new IllegalArgumentException(errorMsg);

                    break;
                }

                default:
                    //noinspection BreakStatementWithLabel
                    break args_loop;
            }
        }

        return new DrCacheArguments(regex, cfg, metrics, cacheFilter, sndGrp, sndGrpName, act, (byte)0);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (arg().action != null)
            return "Warning: this command will change data center replication state for selected caches.";

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrCacheTaskResult res, Logger log) {
        log.info("Data Center ID: " + res.getDataCenterId());

        log.info(DELIM);

        if (res.getDataCenterId() == 0) {
            log.info("Data Replication state: is not configured.");

            return;
        }

        List<String> cacheNames = res.getCacheNames();
        if (cacheNames.isEmpty()) {
            log.info("No matching caches found");

            return;
        }

        log.info(String.format("%d matching cache(s): %s", cacheNames.size(), cacheNames));

        for (String cacheName : cacheNames) {
            List<T2<String, Object>> cacheSndCfg = res.getSenderConfig().get(cacheName);

            printList(log, cacheSndCfg, String.format(
                "Sender configuration for cache \"%s\":",
                cacheName
            ));

            List<T2<String, Object>> cacheRcvCfg = res.getReceiverConfig().get(cacheName);

            printList(log, cacheRcvCfg, String.format(
                "Receiver configuration for cache \"%s\":",
                cacheName
            ));
        }

        for (String cacheName : cacheNames) {
            List<T2<String, Object>> cacheSndMetrics = res.getSenderMetrics().get(cacheName);

            printList(log, cacheSndMetrics, String.format(
                "Sender metrics for cache \"%s\":",
                cacheName
            ));

            List<T2<String, Object>> cacheRcvMetrics = res.getReceiverMetrics().get(cacheName);

            printList(log, cacheRcvMetrics, String.format(
                "Receiver metrics for cache \"%s\":",
                cacheName
            ));
        }

        for (String msg : res.getResultMessages())
            log.info(msg);
    }

    /** */
    private static void printList(Logger log, List<T2<String, Object>> cfg, String s) {
        if (cfg != null && !cfg.isEmpty()) {
            log.info(s);

            for (T2<String, Object> t2 : cfg)
                log.info(String.format(INDENT + "%s=%s", t2.toArray()));
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DrSubCommandsList.CACHE.text();
    }

    /** */
    @SuppressWarnings("PublicInnerClass") public enum CacheFilter {
        /** All. */ ALL,
        /** Sending. */ SENDING,
        /** Receiving. */ RECEIVING,
        /** Paused. */ PAUSED,
        /** Error. */ ERROR
    }

    /** */
    @SuppressWarnings("PublicInnerClass") public enum SenderGroup {
        /** All. */ ALL,
        /** Default. */ DEFAULT,
        /** None. */ NONE;

        /** */
        public static SenderGroup parse(String text) {
            try {
                return valueOf(text.toUpperCase(Locale.ENGLISH));
            }
            catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    /** */
    @SuppressWarnings("PublicInnerClass") public enum Action {
        /** Stop. */ STOP("stop"),
        /** Start. */ START("start"),
        /** Full state transfer. */ FULL_STATE_TRANSFER("full-state-transfer");

        /** String representation. */
        private final String text;

        /** */
        Action(String text) {
            this.text = text;
        }

        /** */
        public String text() {
            return text;
        }

        /** */
        public static Action parse(String text) {
            for (Action action : values()) {
                if (action.text.equalsIgnoreCase(text))
                    return action;
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return text;
        }
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class DrCacheArguments implements DrAbstractRemoteSubCommand.Arguments<VisorDrCacheTaskArgs> {
        /** Regex. */
        private final String regex;
        /** Config. */
        private final boolean config;
        /** Metrics. */
        private final boolean metrics;
        /** Filter. */
        private final CacheFilter filter;
        /** Sender group. */
        private final SenderGroup senderGroup;
        /** Sender group name. */
        private final String senderGroupName;
        /** Action. */
        private final Action action;
        /** Remote data center id. */
        private final byte remoteDataCenterId;

        /** */
        public DrCacheArguments(
            String regex,
            boolean config,
            boolean metrics,
            CacheFilter filter,
            SenderGroup senderGroup,
            String senderGroupName,
            Action action,
            byte remoteDataCenterId
        ) {
            this.regex = regex;
            this.config = config;
            this.metrics = metrics;
            this.filter = filter;
            this.senderGroup = senderGroup;
            this.senderGroupName = senderGroupName;
            this.action = action;
            this.remoteDataCenterId = remoteDataCenterId;
        }

        /** {@inheritDoc} */
        @Override public VisorDrCacheTaskArgs toVisorArgs() {
            return new VisorDrCacheTaskArgs(
                regex,
                config,
                metrics,
                filter.ordinal(),
                senderGroup == null ? VisorDrCacheTaskArgs.SENDER_GROUP_NAMED : senderGroup.ordinal(),
                senderGroupName,
                action == null ? VisorDrCacheTaskArgs.ACTION_NONE : action.ordinal(),
                remoteDataCenterId
            );
        }
    }
}
