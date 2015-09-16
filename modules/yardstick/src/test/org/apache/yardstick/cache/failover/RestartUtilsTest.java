///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.yardstick.cache.failover;
//
//import java.lang.management.ManagementFactory;
//import java.lang.management.RuntimeMXBean;
//import java.util.List;
//import org.apache.ignite.yardstick.cache.failover.RestartUtils;
//import org.yardstickframework.BenchmarkConfiguration;
//
//import static org.yardstickframework.BenchmarkUtils.*;
//import static org.yardstickframework.BenchmarkUtils.jcommander;
//
///**
// * TODO: Add class description.
// */
//public class RestartUtilsTest {
//    public static void main(String[] args) {
////        String kill9 = RestartUtils.kill9("ashutak", "localhost");
//        final BenchmarkConfiguration cfg = new BenchmarkConfiguration();
//
//        String argsFromLog = "-id, 1, -cfg, /home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../config/ignite-localhost-config.xml, " +
//            "-nn, 4, -b, 1, -w, 10, -d, 30, -t, 64, --client, -sm, PRIMARY_SYNC, -dn, IgniteFailoverBenchmark, -sn, IgniteFailoverNode, -ds, failover-atomic, " +
//            "--config, config/failover.properties, --logsFolder, /home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../logs-20150916-165150/logs_servers, --currentFolder, " +
//            "/home/ashutak/dev/incubator-ignite/modules/yardstick, --scriptsFolder, /home/ashutak/dev/incubator-ignite/modules/yardstick/bin], probeWriter='null', " +
//            "customProps={BENCHMARK_PACKAGES=org.yardstickframework,org.apache.ignite.yardstick, DRIVER_HOSTS=localhost,localhost, RESTART_SERVERS=true, " +
//            "CONFIGS=\"-cfg ${SCRIPT_DIR}/../config/ignite-localhost-config.xml -nn ${nodesNum} -b 1 -w 10 -d 30 -t 64 --client -sm PRIMARY_SYNC -dn IgniteFailoverBenchmark -sn IgniteFailoverNode -ds failover-atomic,\", " +
//            "JVM_OPTS=-Xms2g -Xmx2g -Djava.net.preferIPv4Stack=true -DIGNITE_QUIET=false -Xloggc:./gc.log -XX:+PrintGCDetails -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseTLAB -XX:NewSize=128m -XX:MaxNewSize=128m -XX:MaxTenuringThreshold=0 -XX:SurvivorRatio=1024 -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60 -Dyardstick.server1 " +
//            ", ver=\"RELEASE-\", PROPS_ENV=null, SERVER_HOSTS=localhost,localhost}, shutdownHook=true, " +
//            "currentFolder=/home/ashutak/dev/incubator-ignite/modules/yardstick, " +
//            "scriptsFolder=/home/ashutak/dev/incubator-ignite/modules/yardstick/bin, " +
//            "logsFolder=/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../logs-20150916-165150/logs_servers, outputFolder=null, descriptions=[failover-atomic], hostName=]]";
//
//        String[] cmdArgs = new String[]{
//            "--currentFolder", "/home/ashutak/dev/incubator-ignite/modules/yardstick",
//            "--scriptsFolder", "/home/ashutak/dev/incubator-ignite/modules/yardstick/bin",
//            "--logsFolder", "/home/ashutak/dev/incubator-ignite/modules/yardstick/bin/../logs-20150916-165150/logs_servers",
//        };
//
//        cfg.commandLineArguments(cmdArgs);
//
//        cfg.
//
//        jcommander(cmdArgs, cfg, "<benchmark-runner>");
//
//        cfg.customProperties().put("JVM_OPTS", "-Xms2g -Xmx2g -Djava.net.preferIPv4Stack=true -DIGNITE_QUIET=false -Xloggc:./gc.log -XX:+PrintGCDetails -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseTLAB -XX:NewSize=128m -XX:MaxNewSize=128m -XX:MaxTenuringThreshold=0 -XX:SurvivorRatio=1024 -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60 -Dyardstick.server1");
////        cfg.customProperties().put("PROPS_ENV", ""); // TODO see on it.
//
//        RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();
//
//        cfg.customProperties().put("CLASSPATH", ""); // TODO
//
//        RestartUtils.Result result = RestartUtils.start(cfg, true);
//
////        RestartUtils.Result result = RestartUtils.kill9("ashutak", "localhost", 0, true);
//
//        System.out.println(">>> Output>");
//        System.out.println(result.getOutput());
//        System.out.println(">>> Error>");
//        System.out.println(result.getErrorOutput());
//        System.out.println(">>> Exit code: " + result.getExitCode());
//    }
//}
