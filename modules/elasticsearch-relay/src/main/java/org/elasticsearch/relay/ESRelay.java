package org.elasticsearch.relay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.elasticsearch.relay.handler.ESQueryClientIgniteHandler;
import org.elasticsearch.relay.handler.ESQueryHandler;
import org.elasticsearch.relay.handler.ESQueryKernelIgniteHandler;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.model.ESViewQuery;

import org.elasticsearch.relay.util.ESConstants;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * Elasticsearch Relay main servlet taking in all GET and POST requests and
 * their bodies and returning the response created by the ESQueryHandler.
 */
public class ESRelay extends HttpServlet {
	private static final String CONTENT_TYPE = "application/json; charset=UTF-8";

	private final ESRelayConfig fConfig;

	private final Logger fLogger;

	private ESQueryHandler fHandler;
	
	public static ApplicationContext context = null;
	
	public static Map<String, ESViewQuery> allViews = null;
	
	public static GridJettyObjectMapper objectMapper = null;
	
	public static JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(true);

	public ESRelay() {
		fConfig = new ESRelayConfig();

		fLogger = Logger.getLogger(this.getClass().getName());
	}

	@Override
	public void init() throws ServletException {
		// initialize query handler
		try {
			if(context==null)
				context = new ClassPathXmlApplicationContext("realy-*.xml");
			
			allViews = ESRelay.context.getBeansOfType(ESViewQuery.class);
			
			
			if("ignite".equalsIgnoreCase(fConfig.getClusterBackend())){
				
				GridKernalContext ctx = (GridKernalContext) this.getServletContext().getAttribute("gridKernalContext");
				if(ctx==null){
					fHandler = new ESQueryClientIgniteHandler(fConfig);
				}
				else{					
					fHandler = new ESQueryKernelIgniteHandler(fConfig,ctx);
				}
				objectMapper = new GridJettyObjectMapper(ctx);
			}
			else{
				objectMapper = new GridJettyObjectMapper();
				fHandler = new ESQueryHandler(fConfig);
			}
			fLogger.info("init hander:" + fHandler.getClass());
			
		} catch (Exception e) {
			fLogger.severe("init ESRelay:" +e.getMessage());
			throw new ServletException(e);
		}
	}

	private Map<String, String> getParams(HttpServletRequest request) {
		Map<String, String> parameters = new HashMap<String, String>();

		String key = null;
		String value = null;
		Enumeration<?> paramEnum = request.getParameterNames();
		while (paramEnum.hasMoreElements()) {
			key = paramEnum.nextElement().toString();
			value = request.getParameter(key);
			parameters.put(key, value);
		}

		return parameters;
	}

	private String getBody(HttpServletRequest request) {
		StringBuffer buffer = new StringBuffer();

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream(), "utf-8"));

			String line = reader.readLine();
			while (line != null) {
				buffer.append(line);
				buffer.append("\n");
				line = reader.readLine();
			}
		} catch (Exception e) {
			// TODO: ?
			fLogger.log(Level.SEVERE, "failed to read request body", e);
		}

		return buffer.toString();
	}

	/**
	 * index/type/_op 
	 * index/type/{id}
	 * index/type/_bulk
	 * @param request
	 * @return
	 */
	private String[] getFixedPath(HttpServletRequest request) {
		// arrange path elements in a predictable manner

		String pathString = request.getPathInfo();
		while (pathString.startsWith("/") && pathString.length() > 1) {
			pathString = pathString.substring(1);
		}
		String[] path = pathString.split("/");
		if(path.length>=3){
			return path;
		}
		String[] fixedPath = new String[3];
		// search fragment		
		System.arraycopy(path,0,fixedPath,0,path.length);
		fixedPath[2] = ESConstants.SEARCH_FRAGMENT;

		// detect if there is "_search" in there and move it to index 2
		// TODO: possible different structures?

		// indices fragment
		if (path.length > 0 && path[0].charAt(0)!='_') {
			fixedPath[0] = path[0];
		} else if(path.length > 0){			
			fixedPath[2] = path[0];
		}

		// types fragment
		if (path.length > 1 && path[1].charAt(0)!='_') {
			fixedPath[1] = path[1];
		} else if(path.length > 1){				
			fixedPath[2] = path[1];
		}

		// op fragment
		if (path.length > 2 && path[2].charAt(0)=='_') {
			fixedPath[2] = path[2];
		}		
		
		return fixedPath;
	}

	/**
	 * query cmd and ignite rest api
	 * index/type/_cmd
	 */
	@Override
	public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// get authenticated user
		String user = request.getRemoteUser();

		// extract and forward request path
		String[] path = getFixedPath(request);

		// TODO: properly extract query parameters
		Map<String, String> parameters = getParams(request);

		// read request body
		String requestBody = getBody(request);
			
		PrintWriter out = response.getWriter();
		try {
			
			ObjectNode jsonRequest = new ObjectNode(jsonNodeFactory);
			
			if (!requestBody.isEmpty()) {
				if(path[2].equals(ESConstants.BULK_FRAGMENT)){
					String[] batchs = requestBody.split("\\n");
					ArrayNode list = new ArrayNode(jsonNodeFactory);
					for(String line: batchs){
						list.add(objectMapper.readTree(line));
					}
					
					jsonRequest.set(ESConstants.BULK_FRAGMENT, list);
				}
				
				jsonRequest = (ObjectNode) objectMapper.readTree(requestBody);
				//jsonRequest = ObjectNode.parseObject(requestBody);
			}
			
			String result = null;

			//use ignite rest backends  http://127.0.0.1:8080/_cmd/put?cacheName=test&key=k1
			if("_cmd".equalsIgnoreCase(path[0])){
				parameters.put("cmd", path[1]);				
				path[2] = path[1];
				path[1] = path[0];
				path[0] = "ignite";
				ESUpdate query = new ESUpdate(path, parameters, jsonRequest);
				
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
				
			}
			else if(request.getMethod().equalsIgnoreCase("GET") 
					|| path[2].equalsIgnoreCase(ESConstants.SEARCH_FRAGMENT) 
					|| path[2].equalsIgnoreCase(ESConstants.ALL_FRAGMENT)){ // ESConstants._SEARCH ESConstants._ALL
				
				if(path[2].equalsIgnoreCase(ESConstants.ALL_FRAGMENT)) { //sql
					String viewName = parameters.get("q");
					ESViewQuery viewQuery = allViews.get(viewName);
					if(viewQuery==null) { //q is SQL not viewName
						viewQuery = new ESViewQuery(viewName);
					}
					viewQuery.setQueryPath(path);
					viewQuery.setParams(parameters);
					// process request, forward to ES instances
					result = fHandler.handleRequest(viewQuery, user);	
					
				}
				else {
					ESQuery query = new ESQuery(path, parameters, jsonRequest);
					
					// process request, forward to ES instances
					result = fHandler.handleRequest(query, user);	
				}
				
			}
			else if(request.getMethod().equalsIgnoreCase("POST")){
				
				ESUpdate query = new ESUpdate(path, parameters, jsonRequest);
				query.setOp(ESConstants.INSERT_FRAGMENT);
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);
	
			
			}
			else if(request.getMethod().equalsIgnoreCase("PUT")){
				
				ESUpdate query = new ESUpdate(path, parameters, jsonRequest);
				query.setOp(ESConstants.UPDATE_FRAGMENT);
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
			
			}
			else if(request.getMethod().equalsIgnoreCase("DELETE")){
				
				ESUpdate query = new ESUpdate(path, parameters, jsonRequest);
				query.setOp(ESConstants.DELETE_FRAGMENT);
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
			
			}			
			else{				
				throw new UnsupportedOperationException();
			}
			// return result
			response.setContentType(CONTENT_TYPE);
			out.println(result);

		} catch (Exception e) {
			response.setStatus(500);
			response.resetBuffer();
			e.printStackTrace();

			ObjectNode jsonError = new ObjectNode(jsonNodeFactory);
			jsonError.put(ESConstants.R_ERROR, e.toString());
			jsonError.put(ESConstants.R_STATUS, 500);
			fLogger.log(Level.SEVERE, "Error during error JSON generation", e);
			out.println(jsonError.toPrettyString());
		}

		out.flush();
		out.close();
	}
	

	@Override
	public void destroy() {
		// destroy query handler and its threads
		fHandler.destroy();
		fHandler = null;
	}
}