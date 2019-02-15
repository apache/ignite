/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.mesos.resource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.HttpOutput;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * HTTP controller which provides on slave resources.
 */
public class ResourceHandler extends AbstractHandler {
    /** */
    public static final String IGNITE_PREFIX = "/ignite/";

    /** */
    public static final String LIBS_PREFIX = "/libs/";

    /** */
    public static final String CONFIG_PREFIX = "/config/";

    /** */
    public static final String DEFAULT_CONFIG = CONFIG_PREFIX + "default/";

    /** */
    private String libsDir;

    /** */
    private String cfgPath;

    /** */
    private String igniteDir;

    /**
     * @param libsDir Directory with user's libs.
     * @param cfgPath Path to config file.
     * @param igniteDir Directory with ignites.
     */
    public ResourceHandler(String libsDir, String cfgPath, String igniteDir) {
        this.libsDir = libsDir;
        this.cfgPath = cfgPath;
        this.igniteDir = igniteDir;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void handle(
        String url,
        Request request,
        HttpServletRequest httpServletRequest,
        HttpServletResponse response) throws IOException, ServletException {

        String[] path = url.split("/");

        String fileName = path[path.length - 1];

        String servicePath = url.substring(0, url.length() - fileName.length());

        switch (servicePath) {
            case IGNITE_PREFIX:
                handleRequest(response, "application/zip-archive", igniteDir + "/" + fileName);

                request.setHandled(true);
                break;

            case LIBS_PREFIX:
                handleRequest(response, "application/java-archive", libsDir + "/" + fileName);

                request.setHandled(true);
                break;

            case CONFIG_PREFIX:
                handleRequest(response, "application/xml", cfgPath);

                request.setHandled(true);
                break;

            case DEFAULT_CONFIG:
                handleRequest(response, "application/xml",
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName),
                    fileName);

                request.setHandled(true);
                break;
        }
    }

    /**
     * @param response Http response.
     * @param type Type.
     * @param path Path to file.
     * @throws IOException If failed.
     */
    private static void handleRequest(HttpServletResponse response, String type, String path) throws IOException {
        Path path0 = Paths.get(path);

        response.setContentType(type);
        response.setHeader("Content-Disposition", "attachment; filename=\"" + path0.getFileName() + "\"");

        try (HttpOutput out = (HttpOutput)response.getOutputStream()) {
            out.sendContent(FileChannel.open(path0, StandardOpenOption.READ));
        }
    }

    /**
     * @param response Http response.
     * @param type Type.
     * @param stream Stream.
     * @param attachmentName Attachment name.
     * @throws IOException If failed.
     */
    private static void handleRequest(HttpServletResponse response, String type, InputStream stream,
        String attachmentName) throws IOException {
        response.setContentType(type);
        response.setHeader("Content-Disposition", "attachment; filename=\"" + attachmentName + "\"");

        try (HttpOutput out = (HttpOutput)response.getOutputStream()) {
            out.sendContent(stream);
        }
    }
}