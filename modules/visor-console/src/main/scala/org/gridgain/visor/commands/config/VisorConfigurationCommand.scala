/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.config

import org.gridgain.visor._
import org.gridgain.visor.commands.{VisorConsoleCommand, VisorTextTable}
import visor._
import org.gridgain.grid._
import util.{GridUtils => U}
import resources.GridInstanceResource
import org.jetbrains.annotations.Nullable
import collection._
import JavaConversions._
import scala.util.control.Breaks._
import GridSystemProperties._
import org.gridgain.grid.kernal.processors.cache.GridCacheUtils._
import java.lang.System._
import java.util.{Locale, Date}
import java.text._
import scala.reflect.ClassTag
import org.gridgain.grid.lang.GridCallable
import org.gridgain.grid.kernal.GridEx
import org.gridgain.grid.kernal.visor.cmd.VisorConfigurationTask._
import org.gridgain.grid.kernal.visor.cmd.{VisorOneNodeArg, VisorConfigurationTask}

/**
 * ==Overview==
 * Visor 'config' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.config.VisorConfigurationCommand._
 * </ex>
 * Note that `VisorConfigurationCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +-------------------------------------+
 * | config | Prints node configuration. |
 * +-------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     config
 *     config "{-id=<node-id>|id8=<node-id8>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id=<node-id>
 *         Full node ID. Either '-id8' or '-id' can be specified.
 *         If neither is specified - command starts in interactive mode.
 *     -id8=<node-id8>
 *         Node ID8. Either '-id8' or '-id' can be specified.
 *         If neither is specified - command starts in interactive mode.
 * }}}
 *
 * ====Examples====
 * {{{
 *     config "-id8=12345678"
 *         Prints configuration for node with '12345678' ID8.
 *     config
 *         Starts command in interactive mode.
 * }}}
 */
class VisorConfigurationCommand {
    /** Split tag. */
    private val CS = ", "

    /** Default value */
    private val DFLT = "<n/a>"

    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help config' to see how to use this command.")
    }

    /**
     * Joins array elements to string.
     *
     * @param arr Array.
     * @return String.
     */
    private def arr2Str[T: ClassTag](arr: Array[T]): String = {
        if (arr != null && arr.length > 0) U.compact(arr.mkString(", ")) else DFLT
    }

    /**
     * Converts `Boolean` to 'Yes'/'No' string.
     *
     * @param bool Boolean value.
     * @return String.
     */
    private def bool2Str(bool: Boolean): String = {
        if (bool) "on" else "off"
    }

    /**
      * ===Command===
      * Run command in interactive mode.
      *
      * ===Examples===
      * <ex>config</ex>
      * Starts command in interactive mode.
     */
    def config() {
        if (!isConnected)
            adviseToConnect()
        else
            askForNode("Select node from:") match {
                case Some(id) => config("-id=" + id)
                case None => ()
            }
    }

    /**
     * ===Command===
     * Prints configuration of specified node including all caches.
     *
     * ===Examples===
     * <ex>config "-id8=12345678"</ex>
     * Prints configuration of node with '12345678' ID8.
     *
     * @param args Command arguments.
     */
    def config(args: String) {
        breakable {
            if (!isConnected) {
                adviseToConnect()

                break()
            }

            val argLst = parseArgs(args)

            val id8 = argValue("id8", argLst)
            val id = argValue("id", argLst)

            var node: GridNode = null

            if (id8.isEmpty && id.isEmpty) {
                scold("One of -id8 or -id is required.")

                break()
            }

            if (id8.isDefined && id.isDefined) {
                scold("Only one of -id8 or -id is allowed.")

                break()
            }

            if (id8.isDefined) {
                val ns = nodeById8(id8.get)

                if (ns.isEmpty) {
                    scold("Unknown 'id8' value: " + id8.get)

                    break()
                }
                else if (ns.size != 1) {
                    scold("'id8' resolves to more than one node (use full 'id' instead): " + id8.get)

                    break()
                }
                else
                    node = ns.head
            }
            else if (id.isDefined)
                try {
                    node = grid.node(java.util.UUID.fromString(id.get))

                    if (node == null) {
                        scold("'id' does not match any node: " + id.get)

                        break()
                    }
                }
                catch {
                    case e: IllegalArgumentException =>
                        scold("Invalid node 'id': " + id.get)

                        break()
                }

            assert(node != null)

            var cfg: VisorConfiguration = null

            try
                cfg = grid.forNode(node)
                    .compute()
                    .withNoFailover()
                    .execute(classOf[VisorConfigurationTask], new VisorOneNodeArg(node.id()))
                    .get
            catch {
                case e: GridException =>
                    scold(e.getMessage)

                    break()
            }

            println("Common Parameters:")

            val cmnT = VisorTextTable()

            cmnT += ("Grid name", safe(cfg.basic().gridName(), "<default>"))
            cmnT += ("GridGain home", safe(cfg.basic().ggHome(), DFLT))
            cmnT += ("Localhost", safe(cfg.basic().localeHost(), DFLT))
            cmnT += ("Node ID", safe(cfg.basic().nodeId(), DFLT))
            cmnT += ("Marshaller", cfg.basic().marsh())
            cmnT += ("Deployment mode", safe(cfg.basic().deployMode(), DFLT))
            cmnT += ("Daemon", bool2Str(cfg.basic().daemon()))
            cmnT += ("Remote JMX", bool2Str(cfg.basic().jmxRemote()))
            cmnT += ("Restart", bool2Str(cfg.basic().restart()))
            cmnT += ("Network timeout", cfg.basic().networkTimeout() + "ms")
            cmnT += ("License URL", safe(cfg.basic().licenseUrl(), DFLT))
            cmnT += ("Grid logger", safe(cfg.basic().logger(), DFLT))
            cmnT += ("Discovery startup delay", cfg.basic().discoStartupDelay() + "ms")
            cmnT += ("MBean server", safe(cfg.basic().mBeanServer(), DFLT))
            cmnT += ("ASCII logo disabled", bool2Str(cfg.basic().noAscii()))
            cmnT += ("Discovery order not required", bool2Str(cfg.basic().noDiscoOrder()))
            cmnT += ("Shutdown hook disabled", bool2Str(cfg.basic().noShutdownHook()))
            cmnT += ("Program name", safe(cfg.basic(). progName(), DFLT))
            cmnT += ("Quiet mode", bool2Str(cfg.basic().quiet()))
            cmnT += ("Success filename", safe(cfg.basic().successFile(), DFLT))
            cmnT += ("Update notification", bool2Str(cfg.basic().updateNotifier()))
            cmnT += ("Include properties", safe(cfg.inclProperties(), DFLT))

            cmnT.render()

            println("\nLicense:")

            val licT = VisorTextTable()

            if (cfg.license() != null) {
                licT += ("Type", "Enterprise")
                licT += ("ID", safe(cfg.license().id(), DFLT))
                licT += ("Version", safe(cfg.license().version(), DFLT))
                licT += ("Version regular expression", safe(cfg.license().versionRegexp(), DFLT))
                val issueDate = cfg.license().issueDate()
                licT += ("Issue date", if (issueDate != null) formatDate(issueDate) else DFLT)
                licT += ("Issue organization", safe(cfg.license().issueOrganization(), DFLT))
                licT += ("User name", safe(cfg.license().userName(), DFLT))
                licT += ("User organization", safe(cfg.license().userOrganization(), DFLT))
                licT += ("User organization URL", safe(cfg.license().userWww(), DFLT))
                licT += ("User organization e-mail", safe(cfg.license().userEmail(), DFLT))
                licT += ("License note", safe(cfg.license().licenseNote(), DFLT))
                val expireDate = cfg.license().expireDate()
                licT += ("Expire date", if (expireDate != null) formatDate(expireDate) else "No restriction")
                licT += ("Maximum number of nodes", cfg.license().maxNodes())
                licT += ("Maximum number of computers", cfg.license().maxComputers())
                licT += ("Maximum number of CPUs", cfg.license().maxCpus())
                licT += ("Maximum up time", cfg.license().maxUpTime() + " min.")
                licT += ("Grace/burst period", cfg.license().gracePeriod() + " min.")
                licT += ("Disabled subsystems", safe(cfg.license().disabledSubsystems(), "No disabled subsystems"))
            }
            else {
                licT += ("Type", "Open source")
            }

            licT.render()

            println("\nMetrics:")

            val metricsT = VisorTextTable()

            val expTime = cfg.metrics().expectedTime()

            metricsT += ("Metrics expire time", if (expTime != Long.MaxValue) expTime + "ms" else "<never>")
            metricsT += ("Metrics history size", cfg.metrics().historySize())
            metricsT += ("Metrics log frequency", cfg.metrics().loggerFrequency())


            metricsT.render()

            println("\nSPIs:")

            val spisT = VisorTextTable()

            spisT += ("Discovery", safe(cfg.spis().discoSpi(), DFLT))
            spisT += ("Communication", safe(cfg.spis().communicationSpi()))
            spisT += ("Event storage", safe(cfg.spis().eventSpi(), DFLT))
            spisT += ("Collision", safe(cfg.spis().columnSpi(), DFLT))
            spisT += ("Authentication", safe(cfg.spis().authSpi(), DFLT))
            spisT += ("Secure session", safe(cfg.spis().sessionSpi(), DFLT))
            spisT += ("Deployment", safe(cfg.spis().deploySpi(), DFLT))
            spisT += ("Checkpoints", safe(cfg.spis().cpSpis(), DFLT))
            spisT += ("Failovers", safe(cfg.spis().failSpis(), DFLT))
            spisT += ("Load balancings", safe(cfg.spis().loadBalancingSpis(), DFLT))
            spisT += ("Swap spaces", safe(cfg.spis().swapSpaceSpis(), DFLT))

            spisT.render()

            println("\nPeer-to-Peer:")

            val p2pT = VisorTextTable()

            p2pT += ("Peer class loading enabled", bool2Str(cfg.p2p().p2PEnabled()))
            p2pT += ("Missed resources cache size", cfg.p2p().p2PMissedResponseCacheSize())
            p2pT += ("Peer-to-Peer loaded packages", safe(cfg.p2p().p2PLocaleClassPathExcl(), DFLT))

            p2pT.render()

            println("\nEmail:")

            val emailT = VisorTextTable()

            emailT += ("SMTP host", safe(cfg.email().smtpHost(), DFLT))
            emailT += ("SMTP port", safe(cfg.email().smtpPort(), DFLT))
            emailT += ("SMTP username", safe(cfg.email().smtpUsername(), DFLT))
            emailT += ("Admin emails", safe(cfg.email().administrationEmails(), DFLT))
            emailT += ("From email", safe(cfg.email().smtpFromEmail(), DFLT))
            emailT += ("SMTP SSL enabled", bool2Str(cfg.email().smtpSsl()))
            emailT += ("SMTP STARTTLS enabled", bool2Str(cfg.email().smtpStartTls()))

            emailT.render()

            println("\nLifecycle:")

            val lifecycleT = VisorTextTable()

            lifecycleT += ("Beans", safe(cfg.lifecycle().beans(), DFLT))
            lifecycleT += ("Notifications", bool2Str(cfg.lifecycle().notifier()))

            lifecycleT.render()

            println("\nExecutor services:")

            val execSvcT = VisorTextTable()

            execSvcT += ("Executor service", safe(cfg.executeSvc().executeSvc(), DFLT))
            execSvcT += ("Executor service shutdown", safe(cfg.executeSvc().executeSvcShutdown(), DFLT))
            execSvcT += ("System executor service", safe(cfg.executeSvc().systemExecuteSvc(), DFLT))
            execSvcT += ("System executor service shutdown", safe(cfg.executeSvc().systemExecuteSvcShutdown(), DFLT))
            execSvcT += ("Peer-to-Peer executor service", safe(cfg.executeSvc().p2pExecuteSvc(), DFLT))
            execSvcT += ("Peer-to-Peer executor service shutdown", safe(cfg.executeSvc().p2pExecuteSvcShutdown(), DFLT))

            execSvcT.render()

            println("\nSegmentation:")

            val segT = VisorTextTable()

            segT += ("Segmentation policy", safe(cfg.seg().policy(), DFLT))
            segT += ("Segmentation resolvers", cfg.seg().resolvers())
            segT += ("Segmentation check frequency", cfg.seg().checkFrequency())
            segT += ("Wait for segmentation on start", bool2Str(cfg.seg().waitOnStart()))
            segT += ("All resolvers pass required", bool2Str(cfg.seg().passRequired()))

            segT.render()

            println("\nEvents:")

            val evtsT = VisorTextTable()

            val inclEvtTypes = Option(cfg.inclEventTypes()).fold(DFLT)(et => arr2Str(et.map(U.gridEventName)))

            evtsT += ("Included event types", inclEvtTypes)

            evtsT.render()

            println("\nREST:")

            val restT = VisorTextTable()

            restT += ("REST enabled", bool2Str(cfg.restEnabled()))
            restT += ("Jetty path", safe(cfg.jettyPath(), DFLT))
            restT += ("Jetty host", safe(cfg.jettyHost(), DFLT))
            restT += ("Jetty port", safe(cfg.jettyPort(), DFLT))

            restT.render()

            if (cfg.userAttributes().nonEmpty) {
                println("\nUser attributes:")

                val uaT = VisorTextTable()

                uaT #= ("Name", "Value")

                cfg.userAttributes().foreach(a => uaT += (a._1, a._2))

                uaT.render()
            } else
                println("\nNo user attributes defined.")

            if (cfg.env().nonEmpty) {
                println("\nEnvironment variables:")

                val envT = VisorTextTable()

                envT.maxCellWidth = 80

                envT #= ("Name", "Value")

                cfg.env().foreach(v => envT += (v._1, compactProperty(v._1, v._2)))

                envT.render()
            } else
                println("\nNo environment variables defined.")

            val sysProps = cfg.systemProperties().toMap

            if (sysProps.nonEmpty) {
                println("\nSystem properties:")

                val spT = VisorTextTable()

                spT.maxCellWidth = 80

                spT #= ("Name", "Value")

                sysProps.foreach(p => spT += (p._1, compactProperty(p._1, p._2)))

                spT.render()
            } else
                println("\nNo system properties defined.")

            cfg.caches().foreach(cacheCfg => {
                println("\nCache '" + safe(cacheCfg.name(), DFLT) + "':")

                val cacheT = VisorTextTable()

                cacheT += ("Mode", cacheCfg.mode())
                cacheT += ("Time to live", safe(cacheCfg.ttl(), DFLT))
                cacheT += ("Refresh ahead ratio", formatDouble(cacheCfg.refreshAheadRatio()))
                cacheT += ("Atomic sequence reserve size", cacheCfg.sequenceReserveSize())
                cacheT += ("Swap enabled", bool2Str(cacheCfg.swapEnabled()))
                cacheT += ("Batch update", bool2Str(cacheCfg.txBatchUpdate()))
                cacheT += ("Invalidate", bool2Str(cacheCfg.invalidate()))
                cacheT += ("Start size", safe(cacheCfg.startSize(), DFLT))
                cacheT += ("Cloner", cacheCfg.cloner())
                cacheT += ("Transaction manager lookup", cacheCfg.txManagerLookup())
                cacheT += ("Affinity", cacheCfg.affinity().affinity())
                cacheT += ("Affinity mapper", cacheCfg.affinity.affinityMapper())
                cacheT += ("Preload mode", safe(cacheCfg.preload().mode(), DFLT))
                cacheT += ("Preload batch size", cacheCfg.preload().batchSize())
                cacheT += ("Preload thread pool size", cacheCfg.preload().poolSize())
                cacheT += ("Eviction policy", cacheCfg.evict().policy())
                cacheT += ("Eviction key buffer size", cacheCfg.evict().keyBufferSize())
                cacheT += ("Eviction synchronized", bool2Str(cacheCfg.evict().evictSynchronized()))
                cacheT += ("Eviction near synchronized", bool2Str(cacheCfg.evict().nearSynchronized()))
                cacheT += ("Eviction overflow ratio", formatDouble(cacheCfg.evict().maxOverflowRatio()))
                cacheT += ("Near enabled", bool2Str(cacheCfg.near().nearEnabled()))
                cacheT += ("Near start size", cacheCfg.near().nearStartSize())
                cacheT += ("Near eviction policy", cacheCfg.near().nearEvictPolicy())
                cacheT += ("Default isolation", safe(cacheCfg.defaultConfig().defaultIsolation(), DFLT))
                cacheT += ("Default concurrency", safe(cacheCfg.defaultConfig().defaultConcurrency(), DFLT))
                cacheT += ("Default transaction timeout", cacheCfg.defaultConfig().defaultTxTimeout())
                cacheT += ("Default lock timeout", cacheCfg.defaultConfig().defaultLockTimeout())
                cacheT += ("DGC frequency", cacheCfg.dgc().frequency())
                cacheT += ("DGC remove locks flag", bool2Str(cacheCfg.dgc().removedLocks()))
                cacheT += ("DGC suspect lock timeout", cacheCfg.dgc().suspectLockTimeout())
                cacheT += ("Store enabled", bool2Str(cacheCfg.store().enabled()))
                cacheT += ("Store", cacheCfg.store().store())
                cacheT += ("Store values in bytes", bool2Str(cacheCfg.store().valueBytes()))

                cacheT.render()
            })
        }
    }

    /**
     * Splits a string by path separator if it's longer than 100 characters.
     *
     * @param value String.
     * @return List of strings.
     */
    def compactProperty(name: String, value: String): List[String] = {
        val ps = getProperty("path.separator")

        // Split all values having path separator into multiple
        // lines (with few exceptions...).
        val lst =
            if (name != "path.separator" && value.indexOf(ps) != -1 && value.indexOf("http:") == -1 &&
                value.length() > 80)
                value.split(ps).toList
            else
                List(value)

        // Replace whitespaces
        lst.collect {
            case v => v.replaceAll("\n", "<NL>").replaceAll("\r", "<CR>").replaceAll("\t", "<TAB>")
        }
    }
}




/**
 * Closure that returns configuration object.
 */
private class GridConfigurationCallable extends GridCallable[Config] {
    /** Injected grid */
    @GridInstanceResource
    private val g: GridEx = null

    /** Default value */
    private val DFLT = "<n/a>"

    /**
     * Closure body.
     */
    def call(): Config = {
        val c = g.configuration
        val l = g.product().license

        Config(
            // License
            license = License(
                `type` = "Enterprise",
                id = safe(l.id, "<n/a>"),
                ver = safe(l.version, "<n/a>"),
                verRegexp = safe(l.versionRegexp, "<n/a>"),
                issueDate =
                    if (l.issueDate != null)
                        formatDate(l.issueDate)
                    else
                        "<n/a>",
                issueOrg = safe(l.issueOrganization, "<n/a>"),
                userName = safe(l.userName, "<n/a>"),
                userOrg = safe(l.userOrganization, "<n/a>"),
                userWww = safe(l.userWww, "<n/a>"),
                userEmail = safe(l.userEmail, "<n/a>"),
                note = safe(l.licenseNote, "<n/a>"),
                expDate =
                    if (l.expireDate != null)
                        formatDate(l.expireDate)
                    else
                        "No restriction",
                maxNodes =
                    if (l.maxNodes > 0)
                        l.maxNodes.toString
                    else
                        "No restriction",
                maxComp =
                    if (l.maxComputers > 0)
                        l.maxComputers.toString
                    else
                        "No restriction",
                maxCpus =
                    if (l.maxCpus > 0)
                        l.maxCpus.toString
                    else
                        "No restriction",
                maxUpTime =
                    if (l.maxUpTime > 0)
                        l.maxUpTime + " min."
                    else
                        "No restriction",
                gracePeriod =
                    if (l.gracePeriod > 0)
                        l.maxUpTime + " min."
                    else
                        "No grace/burst period",
                disSubs = safe(l.disabledSubsystems, "No disabled subsystems")
            ),

            // Basic
            basic1 = BasicConfig1(
                gridName = safe(c.getGridName, "<default>"),
                ggHome = getProperty(GG_HOME, safe(c.getGridGainHome, DFLT)),
                locHost = getProperty(GG_LOCAL_HOST, safe(c.getLocalHost, DFLT)),
                nodeId = safe(g.localNode().id, DFLT),
                marsh = compactObject(c.getMarshaller),
                deployMode = safe(c.getDeploymentMode, DFLT),
                daemon = bool2Str(boolValue(GG_DAEMON, c.isDaemon)),
                jmxRemote = bool2Str(g.isJmxRemoteEnabled),
                restart = bool2Str(g.isRestartEnabled),
                netTimeout = c.getNetworkTimeout.toString,
                licenseUrl = safe(c.getLicenseUrl, DFLT),
                log = compactObject(c.getGridLogger)
            ),

            basic2 = BasicConfig2(
                discoStartupDelay = c.getDiscoveryStartupDelay.toString,
                mBeanSrv = compactObject(c.getMBeanServer),
                noAscii = bool2Str(boolValue(GG_NO_ASCII, false)),
                noDiscoOrder = bool2Str(boolValue(GG_NO_DISCO_ORDER, false)),
                noShutdownHook = bool2Str(boolValue(GG_NO_SHUTDOWN_HOOK, false)),
                progName = getProperty(GG_PROG_NAME, DFLT),
                quiet = bool2Str(boolValue(GG_QUIET, true)),
                successFile = getProperty(GG_SUCCESS_FILE, DFLT),
                updateNtf = bool2Str(boolValue(GG_UPDATE_NOTIFIER, true))
            ),

            // Metrics
            metrics = MetricsConfig(
                expTime =
                    if (c.getMetricsExpireTime == Long.MaxValue)
                        "<never>"
                    else
                        c.getMetricsExpireTime.toString + "ms",
                historySize = c.getMetricsHistorySize.toString,
                logFreq = c.getMetricsLogFrequency.toString
            ),

            // SPIs
            spis = SpisConfig(
                discoSpi = compactObject(c.getDiscoverySpi),
                commSpi = compactObject(c.getCommunicationSpi),
                evtSpi = compactObject(c.getEventStorageSpi),
                colSpi = compactObject(c.getCollisionSpi),
                authSpi = compactObject(c.getAuthenticationSpi),
                sesSpi = compactObject(c.getSecureSessionSpi),
                deploySpi = compactObject(c.getDeploymentSpi),
                cpSpis = compactArray(c.getCheckpointSpi),
                failSpis = compactArray(c.getFailoverSpi),
                loadBalancingSpis = compactArray(c.getLoadBalancingSpi),
                swapSpaceSpis = compactObject(c.getSwapSpaceSpi)
            ),

            // P2P
            p2p = PeerToPeerConfig(
                p2pEnabled = bool2Str(c.isPeerClassLoadingEnabled),
                p2pMissedResCacheSize = c.getPeerClassLoadingMissedResourcesCacheSize.toString,
                p2pLocClsPathExcl = arr2Str(c.getPeerClassLoadingLocalClassPathExclude)
            ),

            // Email
            email = EmailConfig(
                smtpHost = getProperty(GG_SMTP_HOST, safe(c.getSmtpHost, DFLT)),
                smtpPort = getProperty(GG_SMTP_PORT, safe(c.getSmtpPort.toString, DFLT)),
                smtpUsername = getProperty(GG_SMTP_USERNAME, safe(c.getSmtpUsername, DFLT)),
                adminEmails = getProperty(GG_ADMIN_EMAILS, arr2Str(c.getAdminEmails)),
                smtpFromEmail = getProperty(GG_SMTP_FROM, safe(c.getSmtpFromEmail, DFLT)),
                smtpSsl = bool2Str(boolValue(GG_SMTP_SSL, c.isSmtpSsl)),
                smtpStartTls = bool2Str(boolValue(GG_SMTP_STARTTLS, c.isSmtpStartTls))
            ),

            // Lifecycle
            lifecycle = LifecycleConfig(
                beans = compactArray(c.getLifecycleBeans),
                ntf = bool2Str(boolValue(GG_LIFECYCLE_EMAIL_NOTIFY,
                    c.isLifeCycleEmailNotification))
            ),

            // Executors
            execSvc = ExecServiceConfig(
                execSvc = compactObject(c.getExecutorService),
                execSvcShutdown = bool2Str(c.getExecutorServiceShutdown),
                sysExecSvc = compactObject(c.getSystemExecutorService),
                sysExecSvcShutdown = bool2Str(c.getSystemExecutorServiceShutdown),
                p2pExecSvc = compactObject(c.getPeerClassLoadingExecutorService),
                p2pExecSvcShutdown = bool2Str(c.getPeerClassLoadingExecutorServiceShutdown)
            ),

            // Segmentation
            seg = SegmentationConfig(
                plc = safe(c.getSegmentationPolicy, DFLT),
                resolvers = compactArray(c.getSegmentationResolvers),
                checkFreq = c.getSegmentCheckFrequency.toString,
                waitOnStart = bool2Str(c.isWaitForSegmentOnStart),
                passRequired = bool2Str(c.isAllSegmentationResolversPassRequired)
            ),

            // Include properties
            inclProps =
                if (c.getIncludeProperties != null)
                    arr2Str(c.getIncludeProperties)
                else
                    DFLT,

            // Events
            inclEvtTypes =
                if (c.getIncludeEventTypes != null)
                    arr2Str(c.getIncludeEventTypes.map(U.gridEventName))
                else
                    DFLT,

            // REST
            restEnabled = bool2Str(c.isRestEnabled),
            jettyPath = safe(c.getRestJettyPath, DFLT),
            jettyHost = getProperty(GG_JETTY_HOST, DFLT),
            jettyPort = getProperty(GG_JETTY_PORT, DFLT),

            // User attributes
            userAttrs = strMap(c.getUserAttributes),

            // Caches
            caches = c.getCacheConfiguration.collect {
                case cacheCfg => CacheConfig(
                    name = safe(cacheCfg.getName, DFLT),
                    mode = safe(cacheCfg.getCacheMode, DFLT),
                    ttl = safe(cacheCfg.getDefaultTimeToLive, DFLT),
                    refreshAheadRatio = formatDouble(cacheCfg.getRefreshAheadRatio),
                    seqReserveSize = cacheCfg.getAtomicSequenceReserveSize.toString,
                    swapEnabled = bool2Str(cacheCfg.isSwapEnabled),
                    txBatchUpdate = bool2Str(cacheCfg.isBatchUpdateOnCommit),
                    invalidate = bool2Str(cacheCfg.isInvalidate),
                    startSize = safe(cacheCfg.getStartSize, DFLT),
                    cloner = compactObject(cacheCfg.getCloner),
                    txMgrLookup = cacheCfg.getTransactionManagerLookupClassName,

                    affinity = AffinityConfig(
                        affinity = compactObject(cacheCfg.getAffinity),
                        affinityMapper = compactObject(cacheCfg.getAffinityMapper)
                    ),

                    preload = PreloadConfig(
                        mode = safe(cacheCfg.getPreloadMode, DFLT),
                        batchSize = cacheCfg.getPreloadBatchSize.toString,
                        poolSize = cacheCfg.getPreloadThreadPoolSize.toString
                    ),

                    evict = EvictionConfig(
                        plc = compactObject(cacheCfg.getEvictionPolicy),
                        keyBufSize = cacheCfg.getEvictSynchronizedKeyBufferSize.toString,
                        synchronized = bool2Str(cacheCfg.isEvictSynchronized),
                        nearSynchronized = bool2Str(cacheCfg.isEvictNearSynchronized),
                        maxOverflowRatio = formatDouble(cacheCfg.getEvictMaxOverflowRatio)
                    ),

                    near = NearCacheConfig(
                        nearEnabled = bool2Str(isNearEnabled(cacheCfg)),
                        nearStartSize = cacheCfg.getNearStartSize.toString,
                        nearEvictPlc = compactObject(cacheCfg.getNearEvictionPolicy)
                    ),

                    dflt = DefaultCacheConfig(
                        dfltIsolation = safe(cacheCfg.getDefaultTxIsolation, DFLT),
                        dfltConcurrency = safe(cacheCfg.getDefaultTxConcurrency, DFLT),
                        dfltTxTimeout = cacheCfg.getDefaultTxTimeout.toString,
                        dfltLockTimeout = cacheCfg.getDefaultLockTimeout.toString
                    ),

                    dgc = DgcConfig(
                        freq = cacheCfg.getDgcFrequency.toString,
                        rmvLocks = bool2Str(cacheCfg.isDgcRemoveLocks),
                        suspectLockTimeout = cacheCfg.getDgcSuspectLockTimeout.toString
                    ),

                    store = StoreConfig(
                        enabled = bool2Str(cacheCfg.getStore != null),
                        store = compactObject(cacheCfg.getStore),
                        valueBytes = bool2Str(cacheCfg.isStoreValueBytes)
                    )
                )
            }.toList,

            // Environment
            env = getenv.toMap,

            // System properties
            sysProps = getProperties.toMap
        )
    }

    /** Double formatter. */
    private val dblFmt = new DecimalFormat("#0.00")

    /** Integer formatter. */
    private val intFmt = new DecimalFormat("#0")

    /** Date time format. */
    private val dtFmt = new SimpleDateFormat("MM/dd/yy, HH:mm:ss", Locale.US)

    /** Date format. */
    private val dFmt = new SimpleDateFormat("MM/dd/yy", Locale.US)

    /**
     * Formats double value with `#0.00` formatter.
     *
     * @param d Double value to format.
     */
    def formatDouble(d: Double): String = {
        dblFmt.format(d)
    }

    /**
     * Formats double value with `#0` formatter.
     *
     * @param d Double value to format.
     */
    def formatInt(d: Double): String = {
        intFmt.format(d.round)
    }

    /**
     * Returns string representation of the timestamp provided. Result formatted
     * using pattern `MM/dd/yy, HH:mm:ss`.
     *
     * @param ts Timestamp.
     */
    def formatDateTime(ts: Long): String =
        dtFmt.format(ts)

    /**
     * Returns string representation of the date provided. Result formatted using
     * pattern `MM/dd/yy, HH:mm:ss`.
     *
     * @param date Date.
     */
    def formatDateTime(date: Date): String =
        dtFmt.format(date)

    /**
     * Returns string representation of the timestamp provided. Result formatted
     * using pattern `MM/dd/yy`.
     *
     * @param ts Timestamp.
     */
    def formatDate(ts: Long): String =
        dFmt.format(ts)

    /**
     * Returns string representation of the date provided. Result formatted using
     * pattern `MM/dd/yy`.
     *
     * @param date Date.
     */
    def formatDate(date: Date): String =
        dFmt.format(date)

    /**
     * Gets a non-`null` value for given parameter.
     *
     * @param a Parameter.
     * @param dflt Value to return if `a` is `null`.
     */
    def safe(@Nullable a: Any, dflt: Any = ""): String = {
        assert(dflt != null)

        if (a != null) a.toString else dflt.toString
    }

    /**
     * Returns boolean value from system property or provided function.
     *
     * @param sysPropName System property host.
     * @param f Function that returns `Boolean`.
     * @return `Boolean` value
     */
    private def boolValue(sysPropName: String, f: => Boolean): Boolean = {
        val sysProp = getProperty(sysPropName)

        if (sysProp != null && sysProp.length > 0) sysProp.toBoolean else f
    }

    /**
     * Converts `Boolean` to 'Yes'/'No' string.
     *
     * @param bool Boolean value.
     * @return String.
     */
    private def bool2Str(bool: Boolean): String = {
        if (bool) "on" else "off"
    }

    /**
     * Joins array elements to string.
     *
     * @param arr Array.
     * @return String.
     */
    private def arr2Str[T: ClassTag](arr: Array[T]): String = {
        if (arr != null && arr.length > 0) U.compact(arr.mkString(", ")) else DFLT
    }

    /**
     * Converts values in map to strings.
     *
     * @param map Original map.
     * @return Map with strings.
     */
    private def strMap(map: Map[String, _]): Map[String, String] = {
        map.collect {
            case (k, v) => (k, if (v != null) v.toString else DFLT)
        }
    }

    /**
     * Returns compact class host.
     *
     * @param obj Object to compact.
     * @return String.
     */
    private def compactObject(obj: AnyRef): String = {
        if (obj == null) DFLT else U.compact(obj.getClass.getName)
    }

    /**
     * Returns string representation of SPIs array.
     *
     * @param arr Array of SPIs.
     * @return String.
     */
    private def compactArray[T: ClassTag](arr: Array[T]): String = {
        if (arr == null)
            DFLT
        else
            arr2Str(arr.collect {
                case i: AnyRef => compactObject(i)
            })
    }
}

/**
 * Grid configuration data.
 */
private case class Config (
    // License
    license: License,

    // Basic
    basic1: BasicConfig1,
    basic2: BasicConfig2,

    // Metrics
    metrics: MetricsConfig,

    // SPIs
    spis: SpisConfig,

    // P2P
    p2p: PeerToPeerConfig,

    // Email
    email: EmailConfig,

    // Lifecycle
    lifecycle: LifecycleConfig,

    // Executors
    execSvc: ExecServiceConfig,

    // Segmentation
    seg: SegmentationConfig,

    // Include properties
    inclProps: String,

    // Events
    inclEvtTypes: String,

    // REST
    restEnabled: String,
    jettyPath: String,
    jettyHost: String,
    jettyPort: String,

    // User attributes
    userAttrs: Map[String, String],

    // Caches
    caches: Iterable[CacheConfig],

    // Environment
    env: Map[String, String],

    // System properties
    sysProps: Map[String, String]
)

/**
 * License data.
 */
private case class License(
    `type`: String,
    id: String,
    ver: String,
    verRegexp: String,
    issueDate: String,
    issueOrg: String,
    userName: String,
    userOrg: String,
    userWww: String,
    userEmail: String,
    note: String,
    expDate: String,
    maxNodes: String,
    maxComp: String,
    maxCpus: String,
    maxUpTime: String,
    gracePeriod: String,
    disSubs: String
)

// Basic configuration data is split into
// two classes because of Scala restriction
// of maximum 22 fields per case class.

/**
 * Basic configuration data 1.
 */
private case class BasicConfig1(
    gridName: String,
    ggHome: String,
    locHost: String,
    nodeId: String,
    marsh: String,
    deployMode: String,
    daemon: String,
    jmxRemote: String,
    restart: String,
    netTimeout: String,
    licenseUrl: String,
    log: String
)

/**
 * Basic configuration data 2.
 */
private case class BasicConfig2(
    discoStartupDelay: String,
    mBeanSrv: String,
    noAscii: String,
    noDiscoOrder: String,
    noShutdownHook: String,
    progName: String,
    quiet: String,
    successFile: String,
    updateNtf: String
)

/**
 * Metrics configuration data.
 */
private case class MetricsConfig(
    expTime: String,
    historySize: String,
    logFreq: String
)

/**
 * SPIs configuration data.
 */
private case class SpisConfig(
    discoSpi: String,
    commSpi: String,
    evtSpi: String,
    colSpi: String,
    authSpi: String,
    sesSpi: String,
    deploySpi: String,
    cpSpis: String,
    failSpis: String,
    loadBalancingSpis: String,
    swapSpaceSpis: String
)

/**
 * P2P configuration data.
 */
private case class PeerToPeerConfig(
    p2pEnabled: String,
    p2pMissedResCacheSize: String,
    p2pLocClsPathExcl: String
)

/**
 * Email configuration data.
 */
private case class EmailConfig(
    smtpHost: String,
    smtpPort: String,
    smtpUsername: String,
    adminEmails: String,
    smtpFromEmail: String,
    smtpSsl: String,
    smtpStartTls: String
)

/**
 * Lifecycle configuration data.
 */
private case class LifecycleConfig(
    beans: String,
    ntf: String
)

/**
 * Executors configuration data.
 */
private case class ExecServiceConfig(
    execSvc: String,
    execSvcShutdown: String,
    sysExecSvc: String,
    sysExecSvcShutdown: String,
    p2pExecSvc: String,
    p2pExecSvcShutdown: String
)

/**
 * Segmentation configuration data.
 */
private case class SegmentationConfig(
    plc: String,
    resolvers: String,
    checkFreq: String,
    waitOnStart: String,
    passRequired: String
)

/**
 * Cache configuration data.
 */
private case class CacheConfig(
    name: String,
    mode: String,
    seqReserveSize: String,
    ttl: String,
    refreshAheadRatio: String,
    swapEnabled: String,
    txBatchUpdate: String,
    invalidate: String,
    startSize: String,
    cloner: String,
    txMgrLookup: String,

    affinity: AffinityConfig,
    preload: PreloadConfig,
    evict: EvictionConfig,
    near: NearCacheConfig,
    dflt: DefaultCacheConfig,
    dgc: DgcConfig,
    store: StoreConfig
)

/**
 * Affinity configuration data.
 */
private case class AffinityConfig(
    affinity: String,
    affinityMapper: String
)

/**
 * Preload configuration data.
 */
private case class PreloadConfig(
    poolSize: String,
    mode: String,
    batchSize: String
)

/**
 * Eviction configuration data.
 */
private case class EvictionConfig(
    plc: String,
    keyBufSize: String,
    synchronized: String,
    nearSynchronized: String,
    maxOverflowRatio: String
)

/**
 * Near cache configuration data.
 */
private case class NearCacheConfig(
    nearEnabled: String,
    nearStartSize: String,
    nearEvictPlc: String
)

/**
 * Default cache configuration data.
 */
private case class DefaultCacheConfig(
    dfltIsolation: String,
    dfltConcurrency: String,
    dfltTxTimeout: String,
    dfltLockTimeout: String
)

/**
 * DGC configuration data.
 */
private case class DgcConfig(
    freq: String,
    rmvLocks: String,
    suspectLockTimeout: String
)

/**
 * Store configuration data.
 */
private case class StoreConfig(
    enabled: String,
    store: String,
    valueBytes: String
)

/**
 * Companion object that does initialization of the command.
 */
object VisorConfigurationCommand {
    addHelp(
        name = "config",
        shortInfo = "Prints node configuration.",
        spec = List(
            "config",
            "config {-id=<node-id>|id8=<node-id8>}"
        ),
        args = List(
            "-id=<node-id>" -> List(
                "Full node ID. Either '-id8' or '-id' can be specified.",
                "If neither is specified - command starts in interactive mode."
            ),
            "-id8=<node-id8>" -> List(
                "Node ID8. Either '-id8' or '-id' can be specified.",
                "If neither is specified - command starts in interactive mode.",
                "Note you can also use '@n0' ... '@nn' variables as shortcut to <node-id>."
            )
        ),
        examples = List(
            "config -id8=12345678" ->
                "Prints configuration for node with '12345678' id8.",
            "config -id8=@n0" ->
                "Prints configuration for node with id8 taken from '@n0' memory variable.",
            "config" ->
                "Starts command in interactive mode."
        ),
        ref = VisorConsoleCommand(cmd.config, cmd.config)
    )

    /** Singleton command. */
    private val cmd = new VisorConfigurationCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromConfig2Visor(vs: VisorTag) = cmd
}
