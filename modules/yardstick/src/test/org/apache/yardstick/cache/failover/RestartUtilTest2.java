/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.yardstick.cache.failover;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import org.apache.ignite.yardstick.cache.failover.RestartUtils;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.jcommander;

/**
 * TODO: Add class description.
 */
public class RestartUtilTest2 {
    public static void main(String[] args) {
        final BenchmarkConfiguration cfg = new BenchmarkConfiguration();

        String argsFromLog = "-id, 1, -cfg, /home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../config/ignite-localhost-config.xml, " +
            "-nn, 4, -b, 1, -w, 10, -d, 30, -t, 64, --client, -sm, PRIMARY_SYNC, -dn, IgniteFailoverBenchmark, -sn, IgniteFailoverNode, -ds, failover-atomic, " +
            "--config, config/failover.properties, --logsFolder, /home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../logs-20150916-165150/logs_servers, --currentFolder, " +
            "/home/ashutak/dev/incubator-ignite/modules/yardstick, --scriptsFolder, /home/ashutak/dev/incubator-ignite/modules/yardstick/bin], probeWriter='null', " +
            "customProps={BENCHMARK_PACKAGES=org.yardstickframework,org.apache.ignite.yardstick, DRIVER_HOSTS=localhost,localhost, RESTART_SERVERS=true, " +
            "CONFIGS=\"-cfg ${SCRIPT_DIR}/../config/ignite-localhost-config.xml -nn ${nodesNum} -b 1 -w 10 -d 30 -t 64 --client -sm PRIMARY_SYNC -dn IgniteFailoverBenchmark -sn IgniteFailoverNode -ds failover-atomic,\", " +
            "JVM_OPTS=-Xms2g -Xmx2g -Djava.net.preferIPv4Stack=true -DIGNITE_QUIET=false -Xloggc:./gc.log -XX:+PrintGCDetails -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseTLAB -XX:NewSize=128m -XX:MaxNewSize=128m -XX:MaxTenuringThreshold=0 -XX:SurvivorRatio=1024 -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60 -Dyardstick.server1 " +
            ", ver=\"RELEASE-\", PROPS_ENV=null, SERVER_HOSTS=localhost,localhost}, shutdownHook=true, " +
            "currentFolder=/home/ashutak/dev/incubator-ignite/modules/yardstick, " +
            "scriptsFolder=/home/ashutak/dev/incubator-ignite/modules/yardstick/bin, " +
            "logsFolder=/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../logs-20150916-165150/logs_servers, outputFolder=null, descriptions=[failover-atomic], hostName=]]";

        String[] cmdArgs = new String[]{
            "--currentFolder", "/home/ashutak/dev/incubator-ignite/modules/yardstick",
            "--scriptsFolder", "/home/ashutak/dev/incubator-ignite/modules/yardstick/bin",
            "--logsFolder", "/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../logs-20150916-165150/logs_servers",
            "--remoteuser", "ashutak",
            "--hostName", "localhost",
        };

        cfg.commandLineArguments(cmdArgs);

        jcommander(cmdArgs, cfg, "<benchmark-runner>");

        cfg.customProperties(new HashMap<String, String>());

        cfg.customProperties().put("JVM_OPTS", "-Xms2g -Xmx2g -Djava.net.preferIPv4Stack=true -DIGNITE_QUIET=false -Xloggc:./gc.log -XX:+PrintGCDetails -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseTLAB -XX:NewSize=128m -XX:MaxNewSize=128m -XX:MaxTenuringThreshold=0 -XX:SurvivorRatio=1024 -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60 -Dyardstick.server1");
//        cfg.customProperties().put("PROPS_ENV", ""); // TODO see on it.

        RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();

        cfg.customProperties().put("CLASSPATH", ":/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/commons-codec-1.6.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/ignite-apache-license-gen-1.4.0-SNAPSHOT.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/reflections-0.9.9-RC1.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/guava-11.0.2.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/junit-4.11.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/cache-api-1.0.0.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/javassist-3.16.1-GA.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/ignite-yardstick-1.4.0-SNAPSHOT.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/jcommander-1.32.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/ignite-shmem-1.0.0.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/lucene-core-3.5.0.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/ignite-indexing-1.4.0-SNAPSHOT.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/spring-core-4.1.0.RELEASE.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/spring-beans-4.1.0.RELEASE.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/dom4j-1.6.1.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/xml-apis-1.3.04.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/hamcrest-core-1.3.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/spring-aop-4.1.0.RELEASE.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/aopalliance-1.0.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/tools-1.4.2.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/spring-jdbc-4.1.0.RELEASE.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/jcommon-1.0.21.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/spring-context-4.1.0.RELEASE.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/spring-expression-4.1.0.RELEASE.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/jfreechart-1.0.17.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/yardstick-0.8.0-SNAPSHOT.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/spring-tx-4.1.0.RELEASE.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/h2-1.3.175.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/commons-logging-1.1.1.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/ignite-core-1.4.0-SNAPSHOT.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/ignite-spring-1.4.0-SNAPSHOT.jar:/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../libs/jsr305-1.3.9.jar"); // TODO
        cfg.customProperties().put("JAVA", "/home/ashutak/java/jdk7u75/bin/java");

        RestartUtils.Result result = RestartUtils.start(cfg, true);
//        RestartUtils.Result result = RestartUtils.kill9("ashutak", "localhost", 1, true);

        System.out.println(">>> Output>");
        System.out.println(result.getOutput());
        System.out.println(">>> Error>");
        System.out.println(result.getErrorOutput());
        System.out.println(">>> Exit code: " + result.getExitCode());

        System.out.println(">>>>>>>>>>>RESULT<<<<<<<<<");
        System.out.println(result);
    }
}
