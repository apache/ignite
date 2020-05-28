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

import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.profiling.handlers.CacheOperationsHandler;
import org.apache.ignite.internal.profiling.handlers.ClusterInfoHandler;
import org.apache.ignite.internal.profiling.handlers.ComputeHandler;
import org.apache.ignite.internal.profiling.handlers.IgniteProfilingHandler;
import org.apache.ignite.internal.profiling.handlers.QueryHandler;
import org.apache.ignite.internal.profiling.handlers.TransactionsHandler;
import org.apache.ignite.internal.profiling.util.ProfilingDeserializer;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.regex.Pattern.compile;
import static org.apache.ignite.internal.profiling.util.Utils.MAPPER;
import static org.apache.ignite.internal.util.IgniteUtils.sizeInMegabytes;

/**
 * Profiling files parser. Creates JSONs for UI interface. Builds the report.
 */
public class ProfilingFilesParser {
    /** Profiling file name pattern. */
    private static final Pattern PROFILING_FILE_PATTERN = compile(".*node-(.+).prf$");

    /** Maven-generated archive of UI report resources. */
    private static final String REPORT_RESOURCE_NAME = "report.zip";

    /** File read buffer size. */
    private static final int READ_BUFFER_SIZE = 8 * 1024 * 1024;

    /** File read buffer. */
    private static final ByteBuffer readBuf = allocateDirect(READ_BUFFER_SIZE).order(nativeOrder());

    /** IO factory. */
    private static final RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Node id of current parsed file. */
    private static UUID curNodeId;

    /**
     * @param args Only one argument: profiling logs directory to parse or '-h' to get usage help.
     */
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        String logDir = getLogDir(args);

        HashMap<UUID, File> logs = findLogs(logDir);

        if (logs.isEmpty())
            throw new Exception("Unable to find profiling logs.");

        String resDir = createResultDir(logDir);

        parseLogs(logs, resDir);

        copyReportSources(resDir);

        System.out.println(U.nl() + "Logs parsed successfully [totalTime=" +
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
                    "Usage: profiling.sh path_to_profiling_logs" + U.nl() + U.nl() +
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
    private static HashMap<UUID, File> findLogs(String logDir) throws IOException {
        HashMap<UUID, File> res = new HashMap<>();

        File logsDir = new File(logDir);

        DirectoryStream<Path> files = Files.newDirectoryStream(logsDir.toPath());

        for (Path file : files) {
            Matcher matcher = PROFILING_FILE_PATTERN.matcher(file.toString());

            if (!matcher.matches())
                continue;

            UUID nodeId = UUID.fromString(matcher.group(1));

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
    private static void parseLogs(HashMap<UUID, File> logs, String resDir) throws Exception {
        IgniteProfilingHandler[] handlers = new IgniteProfilingHandler[] {
            new QueryHandler(),
            new CacheOperationsHandler(),
            new TransactionsHandler(),
            new ComputeHandler(),
            new ClusterInfoHandler()
        };

        int currLog = 1;

        for (Map.Entry<UUID, File> entry : logs.entrySet()) {
            UUID nodeId = entry.getKey();
            File log = entry.getValue();

            String progressMsg = "[" + currLog + '/' + logs.size() + " log]";

            curNodeId = nodeId;

            parseLog(handlers, nodeId, log, progressMsg);

            curNodeId = null;

            currLog++;
        }

        ObjectNode dataJson = MAPPER.createObjectNode();

        for (IgniteProfilingHandler handler : handlers)
            handler.results().forEach(dataJson::set);

        writeJsonToFile(resDir + "/data/data.json", dataJson);

        jsonToJsVar(resDir + "/data/data.json", "REPORT_DATA");
    }

    /**
     * Parses node's profiling log.
     *
     * @param handlers Parsers.
     * @param nodeId Node id.
     * @param log Log to parse.
     * @param msg Progress message to log.
     */
    private static void parseLog(IgniteProfilingHandler[] handlers, UUID nodeId, File log, String msg)
        throws Exception {
        System.out.println("Starting parse log [file=" + log.getAbsolutePath() +
            ", size=" + FileUtils.byteCountToDisplaySize(log.length()) + ", nodeId=" + nodeId + ']');

        int infoFrequency = 1_000_000;
        long ts = System.currentTimeMillis();
        long parsed = 0;
        long parsedBytes = 0;

        try (
            FileIO io = ioFactory.create(log);
            ProfilingDeserializer des = new ProfilingDeserializer(handlers)
        ) {
            readBuf.clear();

            while (true) {
                int read = io.read(readBuf);

                readBuf.flip();

                if (read <= 0)
                    break;

                parsedBytes += read;

                while (true) {
                    boolean deserialized = des.deserialize(readBuf);

                    if (!deserialized)
                        break;

                    if (++parsed % infoFrequency == 0) {
                        long speed = infoFrequency / (System.currentTimeMillis() - ts) * 1000;

                        Runtime runtime = Runtime.getRuntime();

                        long memoryUsage = (runtime.totalMemory() - runtime.freeMemory()) * 100 / runtime.maxMemory();

                        System.out.println(msg +
                            " progress: " + parsedBytes * 100 / log.length() + " %," +
                            " speed: " + speed + " ops/sec," +
                            " memory usage: " + memoryUsage + "% of " + sizeInMegabytes(runtime.totalMemory()) + " MB");

                        ts = System.currentTimeMillis();
                    }
                }

                readBuf.compact();
            }
        }

        System.out.println("Log parsed successfully [nodeId=" + nodeId + ']');
    }

    /** @return Node id of current parsed log. */
    public static UUID currentNodeId() {
        return curNodeId;
    }

    /**
     * Writes JSON to file.
     *
     * @param fileName File name path.
     * @param json JSON to write.
     */
    public static void writeJsonToFile(String fileName, JsonNode json) throws IOException {
        ObjectWriter writer = new ObjectMapper().writer(new MinimalPrettyPrinter());

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
        try (InputStream in = ProfilingFilesParser.class.getClassLoader().getResourceAsStream(REPORT_RESOURCE_NAME)) {
            if (in == null) {
                U.delete(new File(resDir));

                throw new RuntimeException("Run from IDE require custom maven assembly (try to package " +
                    "'ignite-profiling' module). The report sources will not be copied to the result directory.");
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
