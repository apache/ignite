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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.VertxModule;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.Vertxlet;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteServicesImpl;
import org.apache.ignite.internal.jackson.IgniteObjectMapper;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.service.GridServiceProxy;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.services.Service;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;

/**
 * Vertx REST handler. The following URL format is supported: {@code /service/:servceName/:action?param1=abc&param2=123} or post json param
 */
public class GridServiceRestHandler extends Vertxlet {
    
	private static final long serialVersionUID = 1L;

	/** Used to sent request charset. */
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    /** */
    private static final String FAILED_TO_PARSE_FORMAT = "Failed to parse parameter of %s type [%s=%s]";

    /** */
    private static final String USER_PARAM = "user";

    /** */
    private static final String PWD_PARAM = "password";

    /** */
    private static final String CACHE_NAME_PARAM = "cacheName";

    /** */
    private static final String BACKUPS_PARAM = "backups";

    /** */
    private static final String CACHE_GROUP_PARAM = "cacheGroup";

    /** */
    private static final String DATA_REGION_PARAM = "dataRegion";

    /** */
    private static final String WRITE_SYNCHRONIZATION_MODE_PARAM = "writeSynchronizationMode";

    /** @deprecated Should be replaced with AUTHENTICATION + token in IGNITE 3.0 */
    private static final String IGNITE_LOGIN = "ignite.login";

    /** @deprecated Should be replaced with AUTHENTICATION + token in IGNITE 3.0 */
    private static final String IGNITE_PASSWORD = "ignite.password";

    /** */
    private static final String TEMPLATE_NAME_PARAM = "templateName";

    /** Logger. */
    private final IgniteLogger log;

    /** Authentication checker. */
    private final IgniteClosure<String, Boolean> authChecker;

    /** Request handlers. */
    private GridRestProtocolHandler hnd;   

    /** Mapper from Java object to JSON. */
    private final ObjectMapper jsonMapper;
    
    private final String contextPath;
    
    private GridKernalContext ctx;



    /**
     * Creates new HTTP requests handler.
     *
     * @param hnd Handler.
     * @param authChecker Authentication checking closure.
     * @param ctx Kernal context.
     */
    GridServiceRestHandler(GridRestProtocolHandler hnd, C1<String, Boolean> authChecker, GridKernalContext ctx) {
        assert hnd != null;
        assert ctx != null;
        this.ctx = ctx;
        this.hnd = hnd;
        this.authChecker = authChecker;
        this.log = ctx.log(getClass());
        this.jsonMapper = new IgniteObjectMapper(ctx);
        this.jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);        
        this.jsonMapper.registerModule(new VertxModule());
        this.contextPath = ctx.igniteInstanceName()==null || ctx.igniteInstanceName().isBlank() ? null: "/"+ctx.igniteInstanceName();

    }   
    
    @Override
    public void handle(RoutingContext rc) {
    	HttpServerRequest srvReq = rc.request();
    	HttpServerResponse res = rc.response();
    	String target = srvReq.path().replaceFirst("/service/", "");
    	
    	handle(target, rc, srvReq, res);
    }

    /** {@inheritDoc} */
    public void handle(String target, RoutingContext rc, HttpServerRequest srvReq, HttpServerResponse res) {
        if (log.isDebugEnabled())
            log.debug("Handling request [serviceTarget=" + target + ", srvReq=" + srvReq + ']');
        
        
        if(target.isBlank())
        	target = "serviceManager";
        
        String action = "call";
        String[] serviceEndponts = target.split("/");
        if(serviceEndponts.length>1) {
        	action = serviceEndponts[1];
        }
        processRequest(serviceEndponts[0],action,rc,srvReq,res);
        
    }

    /**
     * Process HTTP request.
     *
     * @param act Action.
     * @param req Http request.
     * @param res Http response.
     */
    private void processRequest(String serviceName, String action, RoutingContext rc, HttpServerRequest req, HttpServerResponse res) {
        
        res.putHeader("Content-Type", "application/json;charset=UTF-8");
        
        IgniteServicesImpl igniteService = (IgniteServicesImpl)ctx.grid().services();
        boolean sticky = false;
        String strSticky =	req.getHeader("ignite.service.sticky");
        if(strSticky!=null && !strSticky.isBlank()) {
        	sticky = true;
        }
        
        long timeout = IgniteServicesImpl.DFLT_TIMEOUT;
        String strTimeout =	req.getHeader("ignite.service.timeout");
        if(strTimeout!=null && !strTimeout.isBlank()) {
        	timeout = Long.parseLong(strTimeout);
        }
        
        boolean keepBinary = true;

        GridRestResponse cmdRes;

        Map<String, String> params = parameters(req);

        try {        	
        	
            Service serviceObject = ctx.grid().services().service(serviceName);
     		if(serviceObject==null) {
     			res.setStatusCode(404);
     			res.end("service not found!");
                return;
     		}         

            if (!authChecker.apply(req.getHeader("X-Signature"))) {
                res.setStatusCode(401);

                return;
            }
            
            Method method = null;
            Method[] methods = serviceObject.getClass().getDeclaredMethods();
            for(Method m: methods) {
            	int mods = m.getModifiers();
            	if ((mods & Modifier.PUBLIC) != 0 && !Modifier.isStatic(mods)) {
            		if(m.getName().equals(action)) {
            			method = m;
            			break;
            		}
            	}
            }
        	
            if(method==null) {
     			res.setStatusCode(404);
     			res.end("service method not found!");
                return;
     		}
            
        	GridServiceProxy<?> proxy = new GridServiceProxy<>(igniteService.clusterGroup(), serviceName, Service.class, sticky, timeout, ctx, null, keepBinary);

            Object[] argsReq = createRequest(method, params, req, rc);

            if (log.isDebugEnabled())
                log.debug("Initialized service args request: " + argsReq);
            
            Object serviceRes = proxy.invokeMethod(method, argsReq, credentials(params));            

            if (serviceRes != null) {
            	if(serviceRes.getClass().getName().equals("org.apache.ignite.console.agent.service.ServiceResult")) {
            		Object result = U.invoke(serviceRes.getClass(), serviceRes, "getResult");
            		cmdRes = new GridRestResponse(result);
            		Object errorType = U.invoke(serviceRes.getClass(), serviceRes, "getErrorType");
            		if(errorType!=null)
            			cmdRes.setError(errorType.toString());
            		else {            		
	            		List messages = (List)U.invoke(serviceRes.getClass(), serviceRes, "getMessages");
	            		if(messages!=null && !messages.isEmpty())
	            			cmdRes.setError(messages.toString());
            		}
            	}
            	else {
            		cmdRes = new GridRestResponse(serviceRes);
            	}
            }
            else {
            	cmdRes = new GridRestResponse(null);
            }
           
            byte[] sesTok = null;
            
            String sesTokStr = params.get("sessionToken");

            try {
                if (sesTokStr != null) {
                    // Token is a UUID encoded as 16 bytes as HEX.
                    byte[] bytes = U.hexString2ByteArray(sesTokStr);

                    if (bytes.length == 16)
                    	sesTok = bytes;
                }
            }
            catch (IllegalArgumentException ignored) {
                // Ignore invalid session token.
            }
            

            if (sesTok != null)
                cmdRes.setSessionToken(U.byteArray2HexString(sesTok));

            res.setStatusCode(
                cmdRes.getSuccessStatus() == GridRestResponse.SERVICE_UNAVAILABLE  ? 503 : 200
            );
        }
        catch (Throwable e) {
            res.setStatusCode(200);

            U.error(log, "Failed to process HTTP request [action=" + action + ", req=" + req + ']', e);

            if (e instanceof Error)
                throw (Error)e;

            cmdRes = new GridRestResponse(STATUS_FAILED, e.getMessage());
        }

        try (ByteArrayOutputStream os = new ByteArrayOutputStream(1024)) {
            try {
                // Try serialize.               

                jsonMapper.writeValue(os, cmdRes);
                byte[] data = os.toByteArray();
                res.putHeader("Content-Length", String.valueOf(data.length));
                
                res.write(Buffer.buffer(data));
            }
            catch (JsonProcessingException e) {
                U.error(log, "Failed to convert response to JSON: " + cmdRes, e);

                jsonMapper.writeValue(os, new GridRestResponse(STATUS_FAILED, e.getMessage()));
            }

            if (log.isDebugEnabled())
                log.debug("Processed HTTP request [action=" + action + ", jsonRes=" + cmdRes + ", req=" + req + ']');
        }
        catch (IOException e) {
            U.error(log, "Failed to send HTTP response: " + cmdRes, e);
        }
    }

    /**
     * Creates REST request.
     *
     * @param cmd Command.
     * @param params Parameters.
     * @param req Servlet request.
     * @return REST request.
     * @throws IgniteCheckedException If creation failed.
     */
    @Nullable private Object[] createRequest(
    	Method method,
        Map<String, String> params,
        HttpServerRequest req,
        RoutingContext rc
    ) throws IgniteCheckedException {
       
    	
        Class<?>[] parameterTypes = method.getParameterTypes();
        Parameter[] parameter = method.getParameters();
        Object[] args = new Object[method.getParameterCount()];
        
       
        if(req.method()==HttpMethod.POST || req.method()==HttpMethod.PUT) {
        	JsonObject body = rc.body().asJsonObject();
        	
        	for(int i=0;i<args.length;i++) {
            	Class<?> paramType = parameterTypes[i];
            	String name = parameter[i].getName();
            	Object value = body.getValue(name);
            	if(value==null) {
            		continue;
            	}
            	if(paramType.isAssignableFrom(value.getClass())) {
            		args[i] = value;
            		continue;
            	}
            	
            	if (String.class==paramType){
	            	args[i] = body.getString(name);	                
	            }
	            else if (Integer.class==paramType){
	            	args[i] = body.getInteger(name);       
	            }
	            else if (Long.class==paramType){
	            	args[i] = body.getLong(name);       
	            }
	            else if (Short.class==paramType){
	            	args[i] = body.getNumber(name).shortValue();       
	            }
	            else if (Boolean.class==paramType){
	            	args[i] = body.getBoolean(name);
	            }
	            else if (Float.class==paramType){
	            	args[i] = body.getFloat(name);       
	            }
	            else if (Double.class==paramType){
	            	args[i] = body.getDouble(name); 
	            }
	            else if (byte[].class==paramType){
	            	args[i] = body.getBinary(name);
	            }
	            else if (UUID.class==paramType){
	            	args[i] = UUID.fromString(value.toString());
	            }
	            else if (paramType.isAssignableFrom(JsonObject.class)){
	            	args[i] = body.getJsonObject(name);       
	            }
	            else if (paramType.isAssignableFrom(JsonArray.class)){
	            	args[i] = body.getJsonArray(name);
	            }
	            else if (paramType.isAssignableFrom(Map.class)){
	            	args[i] = body.getJsonObject(name).getMap();       
	            }
	            else if (paramType.isAssignableFrom(List.class)){
	            	args[i] = body.getJsonArray(name).getList();   
	            }
	            else if (paramType.isArray() && !paramType.getComponentType().isPrimitive()){
	            	args[i] = body.getJsonArray(name).getList().toArray();   
	            }
	            else if (paramType.isEnum()){
	            	args[i] = Enum.valueOf((Class)paramType,value.toString());
	            }
	            else {
	            	StringConverter converter = new StringConverter(null,jsonMapper);
	            	args[i] = converter.convert(paramType.getName(), value.toString());
	            }
        	}
        }
        else {
        	StringConverter converter = new StringConverter(null,jsonMapper);
        	
        	for(int i=0;i<args.length;i++) {
            	Class<?> paramType = parameterTypes[i];
            	String name = parameter[i].getName();
            	String value = params.get(name);
            	if(value==null) {
            		continue;
            	}
            	if(paramType.isAssignableFrom(value.getClass())) {
            		args[i] = value;
            		continue;
            	}
            	
            	args[i] = converter.convert(paramType.getName(), value);	
        	}
        }        

        return args;
    }

    /**
     * @param params Parameters.
     * @param userParam Parameter name to take user name.
     * @param pwdParam Parameter name to take password.
     * @param restReq Request to add credentials if any.
     * @return {@code true} If params contains credentials.
     */
    private Map<String, Object> credentials(Map<String, String> params) {
        boolean hasCreds = params.containsKey(IGNITE_LOGIN) || params.containsKey(IGNITE_PASSWORD);

        if (hasCreds) {
            SecurityCredentials cred = new SecurityCredentials(params.get(IGNITE_LOGIN), params.get(IGNITE_PASSWORD));

            HashMap<String,Object> attr = new HashMap<String,Object>();
        	attr.put("credentials", cred);
        	return attr;
        }        
        
        hasCreds = params.containsKey(USER_PARAM) || params.containsKey(PWD_PARAM);
        if (hasCreds) {
            SecurityCredentials cred = new SecurityCredentials(params.get(USER_PARAM), params.get(PWD_PARAM));

            HashMap<String,Object> attr = new HashMap<String,Object>();
        	attr.put("credentials", cred);
        	return attr;
        } 
        return null;
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
