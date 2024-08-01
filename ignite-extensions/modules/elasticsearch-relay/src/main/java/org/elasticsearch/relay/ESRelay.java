package org.elasticsearch.relay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.elasticsearch.relay.handler.ESQueryClientIgniteHandler;
import org.elasticsearch.relay.handler.ESQueryHandler;
import org.elasticsearch.relay.handler.ESQueryKernelIgniteHandler;
import org.elasticsearch.relay.model.ESDelete;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.model.ESViewQuery;
import org.elasticsearch.relay.util.ESConstants;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;


/**
 * Elasticsearch Relay main servlet taking in all GET and POST requests and
 * their bodies and returning the response created by the ESQueryHandler.
 */
public class ESRelay extends HttpServlet {
	private static final String CONTENT_TYPE = "application/json; charset=UTF-8";
	
	public static ScheduledExecutorService scheduleExecutorService = Executors.newSingleThreadScheduledExecutor();
	   
	public static GridJettyObjectMapper objectMapper = null;
	
	public static JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(true);
	
	public static ApplicationContext context = null;

	private final ESRelayConfig fConfig;

	private final Logger fLogger;

	private ESQueryHandler fHandler;
	
	
	public Map<String, ESViewQuery> allViews = null;	
	

	public ESRelay() {
		fConfig = new ESRelayConfig();
		fLogger = Logger.getLogger(this.getClass().getName());
	}

	@Override
	public void init() throws ServletException {
		// initialize query handler
		try {
			if(context==null) {
				context = new ClassPathXmlApplicationContext(new String[]{"relay-views.xml","relay-plugins.xml"},true);
			}
			
			allViews = context.getBeansOfType(ESViewQuery.class);			
			
			if("elasticsearch".equalsIgnoreCase(fConfig.getClusterBackend())){				
				objectMapper = new GridJettyObjectMapper();
				fHandler = new ESQueryHandler(fConfig);
			}
			else if("igniteClient".equalsIgnoreCase(fConfig.getClusterBackend())){				
				fHandler = new ESQueryClientIgniteHandler(fConfig);				
				objectMapper = new GridJettyObjectMapper();
			}
			else{				
				GridKernalContext ctx = (GridKernalContext) this.getServletContext().getAttribute("gridKernalContext");
				if(ctx==null){
					fHandler = new ESQueryClientIgniteHandler(fConfig);
				}
				else{						
					fHandler = new ESQueryKernelIgniteHandler(fConfig,ctx);
				}
				objectMapper = new GridJettyObjectMapper(ctx);
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
		String[] value = null;
		Enumeration<?> paramEnum = request.getParameterNames();
		while (paramEnum.hasMoreElements()) {
			key = paramEnum.nextElement().toString();
			value = request.getParameterValues(key);
			if(value.length>1) {
				parameters.put(key, String.join(",",value));
			}
			else {
				parameters.put(key, value[0]);
			}
		}

		return parameters;
	}

	private ObjectNode getJSONBody(String cmd, HttpServletRequest request) {
		
		ObjectNode jsonRequest = new ObjectNode(jsonNodeFactory);

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream(), "utf-8"));

			if(cmd.equals(ESConstants.BULK_FRAGMENT)){
				ArrayNode list = new ArrayNode(jsonNodeFactory);
				String line = reader.readLine();
				while (line != null) {	
					if(!line.isBlank() && line.length()>1) {
						list.add(objectMapper.readTree(line));
					}
					line = reader.readLine();
				}				
				jsonRequest.set(ESConstants.BULK_FRAGMENT, list);
			}
			else {
			
				JsonNode jsonData = objectMapper.readTree(reader);
				if(jsonData.isObject()) {
					jsonRequest = (ObjectNode)jsonData;
				}
				else {
					fLogger.log(Level.SEVERE, "request data is not json object");
				}
			}
			
		} catch (IOException e) {
			return null;
		}
		catch (Exception e) {
			// TODO: ?
			fLogger.log(Level.SEVERE, "failed to read request body", e);
		}

		return jsonRequest;
	}

	/**	 
	 * @param request
	 * @return
	 */
	private String[] getFixedPath(HttpServletRequest request) {
		// arrange path elements in a predictable manner
		String pathString = request.getPathInfo();		
		pathString = StringUtils.trimLeadingCharacter(pathString, '/');
		pathString = StringUtils.trimTrailingCharacter(pathString, '/');
		
		String[] path = pathString.split("/");
		if(path.length==1) {
			String[] fixedPath = new String[2];
			if(path[0].isBlank()) {  // home endpoint
				fixedPath[0] = path[0];
				fixedPath[1] = ESConstants.ALL_FRAGMENT;
			}
			else if(path[0].charAt(0)=='_') {
				fixedPath[0] = "";
				fixedPath[1] = path[0];
			}
			else {
				fixedPath[0] = path[0];
				fixedPath[1] = ESConstants.INDICES_FRAGMENT;
			}
			return fixedPath;
		}
		return path;
	}

	/**
	 * query cmd and ignite rest api
	 * {index}/_cmd or {index}/_doc/{id}
	 */
	@Override
	public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// get authenticated user
		String user = request.getRemoteUser();

		// extract and forward request path
		String[] path = getFixedPath(request);
		String action = path[1];
		// properly extract query parameters
		
		Map<String, String[]> params = request.getParameterMap();
		
		PrintWriter out = response.getWriter();
		try {
			
			
			String result = null;

			//use ignite rest backends  /_cmd/put?cacheName=test&key=k1
			if("_cmd".equalsIgnoreCase(path[0])){
				Map<String, String> parameters = getParams(request);
				parameters.put("cmd", path[1]);				

				// read request body
				ObjectNode jsonRequest = getJSONBody(path[0],request);
					
				ESUpdate query = new ESUpdate(new String[] {"","_cmd"}, parameters, jsonRequest);
				
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
				
			}
			// handle views
			else if("_views".equalsIgnoreCase(path[0])) { // views query /_views/{schema}/{name}
				String viewName = String.join(".", Arrays.copyOfRange(path,1,2));
				ESViewQuery viewQuery = allViews.get(viewName);
				if(viewQuery==null) { // path[1] is schema not viewName,  q is SQL 	
					if(path.length>=3) {
						viewQuery = new ESViewQuery(path[1],path[2],request.getParameter("q"));
					}
					else {
						viewQuery = new ESViewQuery(path[1],request.getParameter("q"));
					}
				}
				else {
					viewQuery = new ESViewQuery(viewQuery);
					viewQuery.setName(viewName);
				}
				viewQuery.setQuery(null);
				viewQuery.setParams(params);
				if(!request.getMethod().equals("GET")){
					ObjectNode jsonRequest = getJSONBody(action,request);
					viewQuery.setQuery(jsonRequest);
				}
				// process request, forward to ES instances
				result = fHandler.handleRequest(viewQuery, user);	
				
			}
			else if(request.getMethod().equals("GET") 
					|| action.equalsIgnoreCase(ESConstants.SEARCH_FRAGMENT) 
					|| action.equalsIgnoreCase(ESConstants.ALL_FRAGMENT)
					|| action.equalsIgnoreCase(ESConstants.GET_FRAGMENT)){
				
				
				if(!request.getMethod().equals("GET")){
					ObjectNode jsonRequest = getJSONBody(action,request);
					ESQuery query = new ESQuery(path, params, jsonRequest);
					
					// process request, forward to ES instances
					result = fHandler.handleRequest(query, user);	
				}
				else {
					// handle search
					ESQuery query = new ESQuery(path, params);
					Map<String, String> parameters = getParams(request);
					ObjectNode jsonRequest = objectMapper.convertValue(parameters, ObjectNode.class);
					query.setQuery(jsonRequest);
					// process request, forward to ES instances
					result = fHandler.handleRequest(query, user);	
				}
				
			}
			else if(request.getMethod().equals("POST")){
				Map<String, String> parameters = getParams(request);
				ObjectNode jsonRequest = getJSONBody(action,request);
				ESUpdate query = new ESUpdate(path, parameters, jsonRequest);
				query.setOp(ESConstants.INSERT_FRAGMENT);
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
			
			}
			else if(request.getMethod().equals("PUT")){
				Map<String, String> parameters = getParams(request);
				ObjectNode jsonRequest = getJSONBody(action,request);
				ESUpdate query = new ESUpdate(path, parameters, jsonRequest);
				query.setOp(ESConstants.UPDATE_FRAGMENT);
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
			
			}
			else if(request.getMethod().equals("DELETE")){
				Map<String, String> parameters = getParams(request);
				ESDelete query = new ESDelete(path, parameters);
				
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
			
			}
			else if(request.getMethod().equals("HEAD")){
				// handle exists
				ESQuery query = new ESQuery(path, params);				
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);
				if(result.isBlank() || result.equals("{}") || result.equals("[]")) {
					throw new NoSuchElementException(Arrays.toString(path));
				}
			}
			else{				
				throw new UnsupportedOperationException();
			}
			// return result
			response.setContentType(CONTENT_TYPE);
			out.println(result);
			
		} catch (NoSuchElementException e) {
			response.setStatus(404);
			response.resetBuffer();

			ObjectNode jsonError = new ObjectNode(jsonNodeFactory);
			jsonError.put(ESConstants.R_ERROR, e.getMessage());
			jsonError.put(ESConstants.R_STATUS, 404);
			
			out.println(jsonError.toPrettyString());
			
			fLogger.log(Level.SEVERE, "Error during error JSON generation", e);
			
		} catch (Exception e) {
			
			response.resetBuffer();

			ObjectNode jsonError = new ObjectNode(jsonNodeFactory);
			jsonError.put(ESConstants.R_ERROR, e.getMessage());
			jsonError.put(ESConstants.R_STATUS, 500);
			if(e.getClass().getSimpleName().indexOf("NotFound")>=0) {
				jsonError.put(ESConstants.R_STATUS, 404);
				response.setStatus(404);
			}
			if(e.getClass().getSimpleName().indexOf("Access")>=0) {
				jsonError.put(ESConstants.R_STATUS, 402);
				response.setStatus(402);
			}
			else {
				response.setStatus(500);
			}
			
			out.println(jsonError.toPrettyString());
			
			fLogger.log(Level.SEVERE, "Error during error JSON generation", e);
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