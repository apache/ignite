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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.Vertxlet;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Vertx REST handler. The following URL format is supported: {@code /ignite?cmd=cmdName&param1=abc&param2=123}
 */
public class GridStaticRestHandler extends Vertxlet {
    
	private static final long serialVersionUID = 1L;

	/** Used to sent request charset. */
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    /** Logger. */
    private final IgniteLogger log;


    /** Request handlers. */
    private GridRestProtocolHandler hnd;

    /** Default page. */
    private volatile String dfltPage;

    /** Favicon. */
    private volatile byte[] favicon;

    
    /**
     * Creates new HTTP requests handler.
     *
     * @param hnd Handler.
     * @param authChecker Authentication checking closure.
     * @param ctx Kernal context.
     */
    GridStaticRestHandler(GridKernalContext ctx) {
        assert hnd != null;
        assert ctx != null;

        this.hnd = hnd;

        this.log = ctx.log(getClass());

        
        // Init default page and favicon.
        try {
            initDefaultPage();

            if (log.isDebugEnabled())
                log.debug("Initialized default page.");
        }
        catch (IOException e) {
            U.warn(log, "Failed to initialize default page: " + e.getMessage());
        }

        try {
            initFavicon();

            if (log.isDebugEnabled())
                log.debug(favicon != null ? "Initialized favicon, size: " + favicon.length : "Favicon is null.");
        }
        catch (IOException e) {
            U.warn(log, "Failed to initialize favicon: " + e.getMessage());
        }
    }


    /**
     * @throws IOException If failed.
     */
    private void initDefaultPage() throws IOException {
        assert dfltPage == null;

        InputStream in = getClass().getResourceAsStream("/rest.html");

        if (in != null) {
            LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in, CHARSET));

            try {
                StringBuilder buf = new StringBuilder(2048);

                for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                    buf.append(line);

                    if (!line.endsWith(" "))
                        buf.append(' ');
                }

                dfltPage = buf.toString();
            }
            finally {
                U.closeQuiet(rdr);
            }
        }
    }

    /**
     * @throws IOException If failed.
     */
    private void initFavicon() throws IOException {
        assert favicon == null;

        InputStream in = getClass().getResourceAsStream("/favicon.ico");

        if (in != null) {
            BufferedInputStream bis = new BufferedInputStream(in);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            try {
                byte[] buf = new byte[2048];

                while (true) {
                    int n = bis.read(buf);

                    if (n == -1)
                        break;

                    bos.write(buf, 0, n);
                }

                favicon = bos.toByteArray();
            }
            finally {
                U.closeQuiet(bis);
            }
        }
    }
    
    @Override
    public void handle(RoutingContext rc) {
    	HttpServerRequest srvReq = rc.request();
    	HttpServerResponse res = rc.response();
    	String target = srvReq.path();
    	handle(target, rc, srvReq, res);
    }

    /** {@inheritDoc} */
    public void handle(String target, RoutingContext rc, HttpServerRequest srvReq, HttpServerResponse res) {
        if (log.isDebugEnabled())
            log.debug("Handling request [target=" + target + ", srvReq=" + srvReq + ']');

        
       if (target.startsWith("/favicon.ico")) {
            if (favicon == null) {
                res.setStatusCode(404);

                rc.end();

                return;
            }

            res.setStatusCode(200);
            res.putHeader("Content-Type", "image/x-icon");
            res.putHeader("Content-Length", String.valueOf(favicon.length));
            res.write(Buffer.buffer(favicon));            

            rc.end();
        }
        else if(target.equals("/")){  //modify@byron 
            if (dfltPage == null) {
                res.setStatusCode(404);
               
                rc.end();

                return;
            }

            res.setStatusCode(200);
            res.putHeader("Content-Type", "text/html");
            
            res.end(dfltPage);

        }
        else{ // continue
        	res.setStatusCode(404).end();
        }
    }

    /**
     * Parses HTTP parameters in an appropriate format and return back map of values to predefined list of names.
     *
     * @param req Request.
     * @return Map of parsed parameters.
     */
    private Map<String, String> parameters(HttpServerRequest req) {
        MultiMap params = req.params();

        if (F.isEmpty(params))
            return Collections.emptyMap();

        Map<String, String> map = U.newHashMap(params.size());

        for (Map.Entry<String, String> entry : req.params())
            map.put(entry.getKey(), entry.getValue());

        return map;
    }

}
