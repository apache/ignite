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

package org.apache.ignite.console.web.controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.regex.Pattern;
import io.swagger.annotations.ApiOperation;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.common.Utils.currentRequestOrigin;
import static org.apache.ignite.internal.util.io.GridFilenameUtils.removeExtension;
import static org.springframework.http.HttpHeaders.CACHE_CONTROL;
import static org.springframework.http.HttpHeaders.CONTENT_DISPOSITION;
import static org.springframework.http.HttpHeaders.EXPIRES;
import static org.springframework.http.HttpHeaders.PRAGMA;

/**
 * Controller for download Web Agent API.
 */
@RestController
public class AgentDownloadController {
    /** Buffer size of 30Mb to handle Web Agent ZIP file manipulations. */
    private static final int BUFFER_SZ = 30 * 1024 * 1024;

    /** Messages accessor. */
    private final MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** */
    @Value("${agent.folder.name:agent_dists}")
    private String agentFolderName;

    /** */
    @Value("${agent.file.regexp:ignite-web-console-agent.*\\.zip}")
    private String agentFileRegExp;

    /**
     * @param user User.
     * @throws Exception If failed.
     */
    @ApiOperation(value = "Download agent archive.")
    @GetMapping(path = "/api/v1/downloads/agent")
    public void load(@AuthenticationPrincipal Account user, HttpServletResponse res) throws Exception {
        Path agentFolder = Paths.get(agentFolderName);

        Pattern ptrn = Pattern.compile(agentFileRegExp);

        Path latestAgentPath = Files.list(agentFolder)
            .filter(f -> !Files.isDirectory(f) && ptrn.matcher(f.getFileName().toString()).matches())
            .max(Comparator.comparingLong(f -> f.toFile().lastModified()))
            .orElseThrow(() -> new FileNotFoundException(messages.getMessage("err.agent-dist-not-found")));

            try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(res.getOutputStream())) {
                String latestAgentFileName = latestAgentPath.getFileName().toString();

                res.addHeader(CACHE_CONTROL, "no-cache, no-store, must-revalidate");
                res.addHeader(PRAGMA, "no-cache");
                res.addHeader(EXPIRES, "0");
                res.addHeader(CONTENT_DISPOSITION, "attachment; filename=\"" + latestAgentFileName + "\"");
                res.setContentType("application/zip");

                // Append "default.properties" to agent ZIP.
                zos.putArchiveEntry(new ZipArchiveEntry(removeExtension(latestAgentFileName) + "/default.properties"));

                String content = String.join("\n",
                    "tokens=" + user.getToken(),
                    "server-uri=" + currentRequestOrigin(),
                    "#Uncomment following options if needed:",
                    "#node-uri=http://localhost:8080",
                    "#node-login=ignite",
                    "#node-password=ignite",
                    "#driver-folder=./jdbc-drivers",
                    "#Uncomment and configure following SSL options if needed:",
                    "#node-key-store=client.jks",
                    "#node-key-store-password=MY_PASSWORD",
                    "#node-trust-store=ca.jks",
                    "#node-trust-store-password=MY_PASSWORD",
                    "#server-key-store=client.jks",
                    "#server-key-store-password=MY_PASSWORD",
                    "#server-trust-store=ca.jks",
                    "#server-trust-store-password=MY_PASSWORD",
                    "#cipher-suites=CIPHER1,CIPHER2,CIPHER3"
                );

                zos.write(content.getBytes(UTF_8));
                zos.closeArchiveEntry();

                try (ZipFile zip = new ZipFile(latestAgentPath.toFile())) {
                    // Make a copy of agent ZIP.
                    zip.copyRawEntries(zos, rawEntry -> true);
                }
            }
            catch (IOException ignored) {
                // No-op.
            }
    }
}
