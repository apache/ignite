package org.apache.ignite.console.agent.code;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import com.stranger.common.config.GenConfig;
import com.stranger.domain.GenTable;
import com.stranger.domain.GenTableData;
import com.stranger.mapper.impl.GenMapperImpl;
import com.stranger.mapper.impl.GenTableColumnMapperImpl;
import com.stranger.service.GenService;
import com.stranger.service.impl.GenServiceImpl;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.IOUtils;

public class CrudUICodeGenerator {

	private final GenService genService;

	/** */
	private final HttpClient httpClient;

	private String serverUri = "http://127.0.0.1:3000";

	public CrudUICodeGenerator(){

		genService = new GenServiceImpl(new GenMapperImpl(),new GenTableColumnMapperImpl());

		httpClient = HttpClient.newBuilder()
				.connectTimeout(Duration.ofSeconds(60))
				.followRedirects(HttpClient.Redirect.NEVER)
				.build();
	}

	private void generatorTable(GenConfig config,Map<String,Object> context) throws IOException {
		try {
			List<GenTable> genTables = genService.selectDbTableList(context);
			List<GenTableData> genTableData = genService.buildTableInfo(genTables,config,context);
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ZipOutputStream zip = new ZipOutputStream(outputStream);
			genService.generatorCode(genTableData, zip);
			IOUtils.closeQuietly(zip);
			FileOutputStream fileOutputStream = new FileOutputStream(config.getFileDownLoadPath());
			fileOutputStream.write(outputStream.toByteArray());
			fileOutputStream.flush();
			fileOutputStream.close();
			System.err.println("<=================代码已经生成=================>");

		} catch (IOException ex) {
			throw ex;
		}
	}
	
	public List<String> generator(String destPath, Map<String,Object> context, Collection<String> validTokens) {
		List<String> message = new ArrayList<>();
		GenConfig config = new GenConfig();
		String pkgPath = "org.demo";
		String name = context.get("name").toString();
		boolean isLastNode = (Boolean)context.getOrDefault("isLastNode",true);
		if(!isLastNode){
			return message;
		}
		boolean demo = (Boolean)context.getOrDefault("demo",false);
		List<Map<String,String>> attributes = (List)context.get("attributes");
		List<String> models = (List)context.get("models");
		List<String> caches = (List)context.get("caches");
		try {
			config.setPackageName(pkgPath);
			config.setFileDownLoadPath(destPath+"src/java");
			config.setAuthor("demo");
			for(String token: validTokens) {
				try {
					JsonObject modelList = getMetadata(this.serverUri, name, token);
					context.put("modelList",modelList);

					JsonObject cacheList = getCacheMetadata(this.serverUri, name, token);
					context.put("cacheList",cacheList);
					break;
				} catch (Throwable e) {
					continue;
				}
			}

			context.put("name",name);
			context.put("demo",demo);
			generatorTable(config,context);
			
		} catch (IOException e) {
			e.printStackTrace();
			message.add(e.getMessage());
		}

		return message;
	}

	/**
	 * @param url Request URL.
	 * @param params Request parameters.
	 * @return Request result.
	 * @throws IOException If failed to parse REST result.
	 * @throws Throwable If failed to send request.
	 */
	public HttpResponse<String> sendMetaServerRequest(String url, JsonObject params,String token) throws Throwable {

		final StringBuilder fields = new StringBuilder();
		if(params!=null) {
			params.getMap().forEach((k, v) ->{
				if(v!=null) {
					if(fields.length()>0) {
						fields.append("&");
					}
					fields.append(k);
					fields.append("=");
					fields.append(URLEncoder.encode(String.valueOf(v), StandardCharsets.UTF_8));
				}
			});
		}
		if(fields.length()>0) {
			url = url+"?"+fields.toString();
		}
		URL urlO = new URL(url);

		HttpRequest request = HttpRequest.newBuilder()
				.uri(urlO.toURI())
				.timeout(Duration.ofMinutes(1))
				.header("Content-Type", "application/json")
				.header("Authorization", "Token "+token)
				.GET()
				.build();

		try {
			HttpResponse<String> res = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
			return res;
		}
		catch (Exception e) {
			throw e.getCause();
		}

	}

	/**
	 *
	 * @param serverUri
	 * @param clusterId
	 * @param token
	 * @return json: id,valueType,tableComment,fields
	 * @throws Throwable
	 */
	public JsonObject getMetadata(String serverUri, String clusterId, String token) throws Throwable {

		String clusterConfigGetUrl = serverUri+"/api/v1/configuration/clusters/";
		HttpResponse<String> modelsResp = sendMetaServerRequest(clusterConfigGetUrl+clusterId+"/models", null,token);
		String body = modelsResp.body();
		if(!body.startsWith("[")) {
			return new JsonObject(body);
		}
		JsonArray models = new JsonArray(body);
		JsonObject result = new JsonObject();
		for(int i=0;i<models.size();i++) {
			String modelConfigGetUrl = serverUri+"/api/v1/configuration/domains/";
			JsonObject model = models.getJsonObject(i);

			HttpResponse<String> modelResp = sendMetaServerRequest(modelConfigGetUrl+model.getString("id"), null,token);
			JsonObject modelJson = new JsonObject(modelResp.body());

			result.put(modelJson.getString("valueType"), modelJson);
		}

		return result;
	}

	/**
	 *
	 * @param serverUri
	 * @param clusterId
	 * @param token
	 * @return json: id,valueType,tableComment,fields
	 * @throws Throwable
	 */
	public JsonObject getCacheMetadata(String serverUri, String clusterId, String token) throws Throwable {

		String clusterConfigGetUrl = serverUri+"/api/v1/configuration/clusters/";
		HttpResponse<String> modelsResp = sendMetaServerRequest(clusterConfigGetUrl+clusterId+"/caches", null,token);
		String body = modelsResp.body();
		if(!body.startsWith("[")) {
			return new JsonObject(body);
		}
		JsonArray models = new JsonArray(body);
		JsonObject result = new JsonObject();
		for(int i=0;i<models.size();i++) {
			String modelConfigGetUrl = serverUri+"/api/v1/configuration/caches/";
			JsonObject model = models.getJsonObject(i);

			HttpResponse<String> modelResp = sendMetaServerRequest(modelConfigGetUrl+model.getString("id"), null,token);
			JsonObject modelJson = new JsonObject(modelResp.body());

			result.put(modelJson.getString("name"), modelJson);
		}

		return result;
	}

}
