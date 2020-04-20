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

package org.apache.ignite.internal.profiling;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.internal.profiling.parsers.CacheNamesParser;
import org.apache.ignite.internal.profiling.parsers.CacheOperationsParser;
import org.apache.ignite.internal.profiling.parsers.ComputeParser;
import org.apache.ignite.internal.profiling.parsers.IgniteLogParser;
import org.apache.ignite.internal.profiling.parsers.QueryParser;
import org.apache.ignite.internal.profiling.parsers.TopologyChangesParser;
import org.apache.ignite.internal.profiling.parsers.TransactionsParser;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.regex.Pattern.compile;

/**
 * Profiling log parser. Creates JSONs for UI interface. Builds the report.
 */
public class ProfilingLogParser {
    /** Profiling log file name pattern. */
    private static final Pattern PROFILING_FILE_PATTERN = compile(".*profiling-(.+).log$");

    /** Maven-generated archive of report resources. */
    private static final String REPORT_RESOURCE_NAME = "report.zip";

    /**
     * @param args Only one argument: profiling logs directory to parse or '-h' to get usage help.
     */
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        String logDir = getLogDir(args);

        HashMap<String, File> logs = findLogs(logDir);

        if (logs.isEmpty())
            throw new Exception("Unable to find profiling logs.");

        String resDir = createResultDir(logDir);

        parseLogs(logs, resDir);

        copyReportSources(resDir);

        System.out.println(U.nl() +"Logs parsed successfully [totalTime=" +
            MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime) + " s]" + U.nl() + U.nl() +
            "Report created [dir=" + resDir + "]" + U.nl() +
            "Open '" + resDir + "/index.html' in browser to see the report.");
    }

    /**
     * Extracts log directory from arguments and checks it existing.
     *
     * @param args Arguments to parse.
     * @return Log directory. {@code Null} in case of help required.
     */
    private static String getLogDir(String[] args) {
        if (args.length == 0 || "--help".equalsIgnoreCase(args[0]) || "-h".equalsIgnoreCase(args[0])) {
            System.out.println(
                "The script is used to create a performance report from profiling logs." + U.nl() + U.nl() +
                    "Usage: profiling.sh path_to_profiling_logs"  + U.nl() + U.nl() +
                    "The path should contain profiling logs collected from the cluster." + U.nl() +
                    "Profiling log file name mask: profiling-${sys:nodeId}.log" + U.nl() +
                    "The report will be created at logs path with new directory: " +
                    "path_to_profiling_logs/report_yyyy-MM-dd_HH-mm-ss/" + U.nl() +
                    "Open 'report_yyyy-MM-dd_HH-mm-ss/index.html' in browser to see the report.");

            System.exit(0);
        }

        A.ensure(args.length <= 1, "Too much arguments [args=" + Arrays.toString(args) + ']');

        String logDir = args[0];

        File dir = new File(logDir);

        A.ensure(dir.exists(), "Log directory does not exists.");
        A.ensure(dir.isDirectory(), "Log directory is not a directory.");

        return logDir;
    }

    /**
     * Finds logs to parse.
     *
     * @param logDir Logs directory.
     * @return Map of found logs: nodeId -> log file path.
     */
    private static HashMap<String, File> findLogs(String logDir) throws IOException {
        HashMap<String, File> res = new HashMap<>();

        File logsDir = new File(logDir);

        DirectoryStream<Path> files = Files.newDirectoryStream(logsDir.toPath());

        for (Path file : files) {
            Matcher matcher = PROFILING_FILE_PATTERN.matcher(file.toString());

            if (!matcher.matches())
                continue;

            String nodeId = matcher.group(1);

            res.put(nodeId, file.toFile());

            System.out.println("Found log to parse [path=" + file.toAbsolutePath() + ", nodeId=" + nodeId + ']');
        }

        return res;
    }

    /**
     * @param logDir Parent directory of report results.
     * @return Created result directory.
     */
    private static String createResultDir(String logDir) throws IOException {
        String postfix = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());

        String dir = Files.createDirectory(new File(logDir + "/report_" + postfix).toPath())
            .toAbsolutePath().toString();

        // Data directory.
        Files.createDirectory(new File(dir + "/data").toPath());

        System.out.println("Result directory created [dir=" + dir + ']');

        return dir;
    }

    /**
     * Parses logs.
     *
     * @param logs Profiling logs.
     * @param resDir Results directory.
     */
    private static void parseLogs(HashMap<String, File> logs, String resDir) throws Exception {
        IgniteLogParser[] parsers = new IgniteLogParser[] {
            new QueryParser(),
            new CacheNamesParser(),
            new CacheOperationsParser(),
            new TransactionsParser(),
            new ComputeParser(),
            new TopologyChangesParser(logs.keySet())
        };

        for (byte phase = 1; phase <= 2; phase++) {
            int currLog = 1;

            for (Map.Entry<String, File> entry : logs.entrySet()) {
                String nodeId = entry.getKey();
                File log = entry.getValue();

                String progressMsg = "[" + currLog + '/' + logs.size() + " log]";

                parseLog(parsers, nodeId, log, phase, progressMsg);

                currLog++;
            }

            if (phase == 1) {
                for (IgniteLogParser parser : parsers)
                    parser.onFirstPhaseEnd();
            }
        }

        for (IgniteLogParser parser : parsers) {
            for (Map.Entry<String, JsonNode> entry : parser.results().entrySet()) {
                String fileName = entry.getKey();
                JsonNode json = entry.getValue();

                writeJsonToFile(resDir + "/data/" + fileName + ".json", json);

                jsonToJsVar(resDir + "/data/" + fileName + ".json", "report_" + fileName);
            }
        }
    }

    /**
     * Parses node's profiling log.
     *
     * @param parsers Parsers.
     * @param nodeId Node id.
     * @param log Log to parse.
     * @param phase Phase.
     * @param msg Progress message to log.
     */
    private static void parseLog(IgniteLogParser[] parsers, String nodeId, File log, byte phase, String msg)
        throws Exception {
        System.out.println("Starting parse log [file=" + log.getAbsolutePath() +
            ", size=" + FileUtils.byteCountToDisplaySize(log.length()) + ", nodeId=" + nodeId + ']');

        long ts = System.currentTimeMillis();
        long parsed = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(log))) {
            for (String line; (line = br.readLine()) != null; ) {
                for (IgniteLogParser parser : parsers) {
                    if (phase == 1)
                        parser.parse(nodeId, line);
                    else
                        parser.parsePhase2(nodeId, line);
                }

                if (++parsed % 1_000_000 == 0) {
                    long speed = 1_000_000 / (System.currentTimeMillis() - ts) * 1000;

                    Runtime runtime = Runtime.getRuntime();

                    long memoryUsage = (runtime.totalMemory() - runtime.freeMemory()) * 100 / runtime.maxMemory();

                    System.out.println("[" + phase + "/2 phase] " + msg +
                        " parsed: " + parsed / 1_000_000 + " MM lines," +
                        " speed: " + speed + " lines/sec," +
                        " memory usage: " + memoryUsage + "% of " + U.sizeInMegabytes(runtime.totalMemory()) + " MB");

                    ts = System.currentTimeMillis();
                }
            }
        }

        System.out.println("Log parsed successfully [nodeId=" + nodeId + ']');
    }

    /**
     * Writes JSON to file.
     *
     * @param fileName File name path.
     * @param json JSON to write.
     */
    public static void writeJsonToFile(String fileName, JsonNode json) throws IOException {
        ObjectWriter writer = new ObjectMapper().writer(new DefaultPrettyPrinter());

        File file = new File(fileName);

        writer.writeValue(file, json);

        System.out.println("Json writed to file [file=" + file.getAbsolutePath() + ']');
    }

    /**
     * Make JSON as JS variable (CORS blocks external json files).
     *
     * @param fileName The JSON file path.
     * @param varName The variable name for the JSON object.
     */
    public static void jsonToJsVar(String fileName, String varName) throws IOException {
        File jsonFile = new File(fileName);
        File jsFile = new File(fileName + ".js");

        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(jsFile))) {
            fileWriter.write("var " + varName + " = ");

            try (BufferedReader reader = new BufferedReader(new FileReader(jsonFile))) {
                String line;

                while ((line = reader.readLine()) != null) {
                    fileWriter.write(line);
                    fileWriter.newLine();
                }
            }
        }

        jsonFile.delete();

        System.out.println("Json converted to js [file=" + jsonFile.getAbsolutePath() + ".js]");
    }

    /**
     * Unzip and copies UI files to result directory.
     *
     * @param resDir Directory to copy sources to.
     */
    private static void copyReportSources(String resDir) throws Exception {
        try (InputStream in = ProfilingLogParser.class.getClassLoader().getResourceAsStream(REPORT_RESOURCE_NAME)) {
            if (in == null) {
                // TODO Run from IDE require custom maven assembly (try to package module).
                System.err.println("Run from IDE require custom maven assembly (try to package " +
                    "'ignite-profiling' module). Report sources will not be copied to the result dir.");

                return;
            }

            try (ZipInputStream zip = new ZipInputStream(in)) {
                ZipEntry entry;

                while ((entry = zip.getNextEntry()) != null) {
                    File entryDestination = new File(resDir, entry.getName());

                    if (entry.isDirectory())
                        entryDestination.mkdirs();
                    else {
                        entryDestination.getParentFile().mkdirs();

                        try (OutputStream out = new FileOutputStream(entryDestination)) {
                            IOUtils.copy(zip, out);
                        }
                    }
                }
            }
        }
    }
}