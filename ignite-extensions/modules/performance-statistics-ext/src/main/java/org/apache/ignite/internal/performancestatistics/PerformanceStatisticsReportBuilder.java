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

package org.apache.ignite.internal.performancestatistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.internal.performancestatistics.handlers.CacheOperationsHandler;
import org.apache.ignite.internal.performancestatistics.handlers.ClusterInfoHandler;
import org.apache.ignite.internal.performancestatistics.handlers.ComputeHandler;
import org.apache.ignite.internal.performancestatistics.handlers.IgnitePerformanceStatisticsHandler;
import org.apache.ignite.internal.performancestatistics.handlers.QueryHandler;
import org.apache.ignite.internal.performancestatistics.handlers.TransactionsHandler;
import org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsReader;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.performancestatistics.util.Utils.MAPPER;

/**
 * Performance statistics report builder. Parses files and creates JSONs for UI interface. Builds the report.
 */
public class PerformanceStatisticsReportBuilder {
    /** Maven-generated archive of UI report resources. */
    private static final String REPORT_RESOURCE_NAME = "report.zip";

    /**
     * @param args Only one argument: Performance statistics files directory to parse or '-h' to get usage help.
     */
    public static void main(String... args) throws Exception {
        String filesDir = parseArguments(args);

        String resDir = createResultDir(filesDir);

        createReport(filesDir, resDir);

        System.out.println("Report created [dir=" + resDir + "]" + U.nl() +
            "Open '" + resDir + "/index.html' in browser to see the report.");
    }

    /**
     * Parses arguments and print help or extracts files directory from arguments and checks it existing.
     *
     * @param args Arguments to parse.
     * @return Performance statistics files directory.
     */
    private static String parseArguments(String[] args) {
        if (args == null || args.length == 0 || "--help".equalsIgnoreCase(args[0]) || "-h".equalsIgnoreCase(args[0])) {
            System.out.println(
                "The script is used to create a performance report from performance statistics files." +
                    U.nl() + U.nl() +
                    "Usage: build-report.sh path_to_files" +
                    U.nl() + U.nl() +
                    "The path should contain performance statistics files collected from the cluster." + U.nl() +
                    "Performance statistics file name mask: node-${sys:nodeId}.prf" + U.nl() +
                    "The report will be created at files path with new directory: " +
                    "path_to_files/report_yyyy-MM-dd_HH-mm-ss/" + U.nl() +
                    "Open 'report_yyyy-MM-dd_HH-mm-ss/index.html' in browser to see the report.");

            System.exit(0);
        }

        A.ensure(args.length <= 1, "Too much arguments [args=" + Arrays.toString(args) + ']');

        String filesDir = args[0];

        File dir = new File(filesDir);

        A.ensure(dir.exists(), "Performance statistics files directory does not exists.");
        A.ensure(dir.isDirectory(), "Performance statistics files directory is not a directory.");

        return filesDir;
    }

    /**
     * Creates performance report.
     *
     * @param filesDir Performance statistics files.
     * @param resDir Results directory.
     */
    private static void createReport(String filesDir, String resDir) throws Exception {
        IgnitePerformanceStatisticsHandler[] handlers = new IgnitePerformanceStatisticsHandler[] {
            new QueryHandler(),
            new CacheOperationsHandler(),
            new TransactionsHandler(),
            new ComputeHandler(),
            new ClusterInfoHandler()
        };

        new FilePerformanceStatisticsReader(handlers).read(Collections.singletonList(new File(filesDir)));

        ObjectNode dataJson = MAPPER.createObjectNode();

        for (IgnitePerformanceStatisticsHandler handler : handlers)
            handler.results().forEach(dataJson::set);

        writeJsonToFile(resDir + "/data/data.json", dataJson);

        jsonToJsVar(resDir + "/data/data.json", "REPORT_DATA");

        copyReportSources(resDir);
    }

    /**
     * @param filesDir Parent directory of report results.
     * @return Created result directory.
     */
    private static String createResultDir(String filesDir) throws IOException {
        String postfix = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());

        String resDir = Files.createDirectory(new File(filesDir, "report_" + postfix).toPath())
            .toAbsolutePath().toString();

        // Data directory.
        Files.createDirectory(new File(resDir, "data").toPath());

        return resDir;
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
    }

    /**
     * Unzip and copies UI files to result directory.
     *
     * @param resDir Directory to copy sources to.
     */
    private static void copyReportSources(String resDir) throws Exception {
        try (InputStream in = PerformanceStatisticsReportBuilder.class.getClassLoader().getResourceAsStream(REPORT_RESOURCE_NAME)) {
            if (in == null) {
                System.err.println("Run from IDE require custom maven assembly to create UI resources (try " +
                    "to package 'ignite-performance-statistics-ext' module or set up executing 'package' phase " +
                    "before build). The report sources will not be copied to the result directory.");

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
