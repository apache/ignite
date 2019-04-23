package org.apache.ignite.internal.commandline.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTask;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTask;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskResult;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskV2;
import org.apache.ignite.lang.IgniteProductVersion;

import static java.lang.String.format;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;

public class IdleVerify extends Command<IdleVerify.Arguments> {
    public static class Arguments {
        /** Caches. */
        private Set<String> caches;

        /** Exclude caches or groups. */
        private Set<String> excludeCaches;

        /** Calculate partition hash and print into standard output. */
        private boolean dump;

        /** Skip zeros partitions. */
        private boolean skipZeros;

        /** Check CRC sum on idle verify. */
        private boolean idleCheckCrc;

        /** Cache filter. */
        private CacheFilterEnum cacheFilterEnum = CacheFilterEnum.ALL;

        public Arguments(Set<String> caches, Set<String> excludeCaches, boolean dump, boolean skipZeros,
            boolean idleCheckCrc,
            CacheFilterEnum cacheFilterEnum) {
            this.caches = caches;
            this.excludeCaches = excludeCaches;
            this.dump = dump;
            this.skipZeros = skipZeros;
            this.idleCheckCrc = idleCheckCrc;
            this.cacheFilterEnum = cacheFilterEnum;
        }

        /**
         * @return Gets filter of caches, which will by checked.
         */
        public CacheFilterEnum getCacheFilterEnum() {
            return cacheFilterEnum;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Exclude caches or groups.
         */
        public Set<String> excludeCaches() {
            return excludeCaches;
        }

        /**
         * @return Calculate partition hash and print into standard output.
         */
        public boolean dump() {
            return dump;
        }

        /**
         * @return Check page CRC sum on idle verify flag.
         */
        public boolean idleCheckCrc() {
            return idleCheckCrc;
        }


        /**
         * @return Skip zeros partitions(size == 0) in result.
         */
        public boolean isSkipZeros() {
            return skipZeros;
        }
    }

    private Arguments args;

    @Override public Arguments arg() {
        return args;
    }

    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        try (GridClient client = startClient(clientCfg);) {
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

            if (args.dump())
                cacheIdleVerifyDump(client, clientCfg, logger);
            else if (idleVerifyV2)
                cacheIdleVerifyV2(client, clientCfg);
            else
                legacyCacheIdleVerify(client, clientCfg, logger);
        }

        return null;
    }

    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> cacheNames = null;
        boolean dump = false;
        boolean skipZeros = false;
        boolean idleCheckCrc = false;
        CacheFilterEnum cacheFilterEnum = CacheFilterEnum.ALL;
        Set<String> excludeCaches = null;

        int idleVerifyArgsCnt = 5;

        while (argIter.hasNextSubArg() && idleVerifyArgsCnt-- > 0) {
            String nextArg = argIter.nextArg("");

            IdleVerifyCommandArg arg = CommandArgUtils.of(nextArg, IdleVerifyCommandArg.class);

            if (arg == null) {
                cacheNames = argIter.parseStringSet(nextArg);

                validateRegexes(cacheNames);
            }
            else {
                switch (arg) {
                    case DUMP:
                        dump = true;

                        break;

                    case SKIP_ZEROS:
                        skipZeros = true;

                        break;

                    case CHECK_CRC:
                        idleCheckCrc = true;

                        break;

                    case CACHE_FILTER:
                        String filter = argIter.nextArg("The cache filter should be specified. The following " +
                            "values can be used: " + Arrays.toString(CacheFilterEnum.values()) + '.');

                        cacheFilterEnum = CacheFilterEnum.valueOf(filter.toUpperCase());

                        break;

                    case EXCLUDE_CACHES:
                        excludeCaches = argIter.nextStringSet("caches, which will be excluded.");

                        validateRegexes(excludeCaches);

                        break;
                }
            }
        }

        args = new Arguments(cacheNames, excludeCaches, dump, skipZeros, idleCheckCrc, cacheFilterEnum);
    }

    private void validateRegexes(Set<String> cacheNames) {
        cacheNames.forEach(c -> {
            try {
                Pattern.compile(c);
            }
            catch (PatternSyntaxException e) {
                throw new IgniteException(format("Invalid cache name regexp '%s': %s", c, e.getMessage()));
            }
        });
    }

    /**
     * @param client Client.
     * @param clientCfg Client configuration.
     */
    private void cacheIdleVerifyDump(
        GridClient client,
        GridClientConfiguration clientCfg,
        CommandLogger logger
    ) throws GridClientException {
        VisorIdleVerifyDumpTaskArg arg = new VisorIdleVerifyDumpTaskArg(
            args.caches(),
            args.excludeCaches(),
            args.isSkipZeros(),
            args.getCacheFilterEnum(),
            args.idleCheckCrc()
        );

        String path = executeTask(client, VisorIdleVerifyDumpTask.class, arg, clientCfg);

        logger.log("VisorIdleVerifyDumpTask successfully written output to '" + path + "'");
    }


    /**
     * @param client Client.
     * @param clientCfg Client configuration.
     */
    private void cacheIdleVerifyV2(
        GridClient client,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        IdleVerifyResultV2 res = executeTask(
            client,
            VisorIdleVerifyTaskV2.class,
            new VisorIdleVerifyTaskArg(args.caches(), args.excludeCaches(), args.idleCheckCrc()),
            clientCfg);

        res.print(System.out::print);
    }


    /**
     * @param client Client.
     * @param clientCfg Client configuration.
     */
    private void legacyCacheIdleVerify(
        GridClient client,
        GridClientConfiguration clientCfg,
        CommandLogger logger
    ) throws GridClientException {
        VisorIdleVerifyTaskResult res = executeTask(
            client,
            VisorIdleVerifyTask.class,
            new VisorIdleVerifyTaskArg(args.caches(), args.excludeCaches(), args.idleCheckCrc()),
            clientCfg);

        Map<PartitionKey, List<PartitionHashRecord>> conflicts = res.getConflicts();

        if (conflicts.isEmpty()) {
            logger.log("idle_verify check has finished, no conflicts have been found.");
            logger.nl();
        }
        else {
            logger.log("idle_verify check has finished, found " + conflicts.size() + " conflict partitions.");
            logger.nl();

            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : conflicts.entrySet()) {
                logger.log("Conflict partition: " + entry.getKey());

                logger.log("Partition instances: " + entry.getValue());
            }
        }
    }
}
