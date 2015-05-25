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

package org.apache.ignite.mesos.resource;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.*;

/**
 * HTTP controller which provides on slave resources.
 */
@Path("/")
public class ResourceController {
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
     * @param libsDir Path to directory with user libs.
     * @param cfgPath Path to config file.
     */
    public ResourceController(String libsDir, String cfgPath, String igniteDir) {
        this.libsDir = libsDir;
        this.cfgPath = cfgPath;
        this.igniteDir = igniteDir;
    }

    /**
     * @param ignite Ignite jar name.
     * @return Http response.
     */
    @GET
    @Path(IGNITE_PREFIX + "{ignite-dist}")
    public Response ignite(@PathParam("ignite-dist") String ignite) {
        return handleRequest(new File(igniteDir + "/" + ignite), "application/zip-archive", ignite);
    }

    /**
     * @param lib user's jar.
     * @return Http response.
     */
    @GET
    @Path(LIBS_PREFIX + "{lib}")
    public Response lib(@PathParam("lib") String lib) {
        return handleRequest(new File(libsDir + "/" + lib), "application/java-archive", lib);
    }

    /**
     *
     * @param cfg Config file.
     * @return Http response.
     */
    @GET
    @Path(CONFIG_PREFIX + "{cfg}")
    public Response config(@PathParam("cfg") String cfg) {
        return handleRequest(new File(cfgPath), "application/xml", cfg);
    }

    /**
     * @param cfg Config file.
     * @return Http response.
     */
    @GET
    @Path(DEFAULT_CONFIG + "{cfg}")
    public Response defaultConfig(@PathParam("cfg") String cfg) {
        return handleRequest(Thread.currentThread().getContextClassLoader().getResourceAsStream(cfg),
            "application/xml", cfg);
    }

    /**
     *
     * @param resource File resource.
     * @param type Type.
     * @param attachmentName Attachment name.
     * @return Http response.
     */
    private static Response handleRequest(File resource, String type, String attachmentName) {
        final Response.ResponseBuilder builder = Response.ok(resource, type);
        builder.header("Content-Disposition", "attachment; filename=\"" + attachmentName + "\"");
        return builder.build();
    }

    /**
     *
     * @param resource File resource.
     * @param type Type.
     * @param attachmentName Attachment name.
     * @return Http response.
     */
    private static Response handleRequest(InputStream resource, String type, String attachmentName) {
        final Response.ResponseBuilder builder = Response.ok(resource, type);
        builder.header("Content-Disposition", "attachment; filename=\"" + attachmentName + "\"");
        return builder.build();
    }
}
