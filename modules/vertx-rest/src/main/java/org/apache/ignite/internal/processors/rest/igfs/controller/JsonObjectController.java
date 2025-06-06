package org.apache.ignite.internal.processors.rest.igfs.controller;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.webmvc.VertxInstanceAware;
import io.vertx.webmvc.annotation.Blocking;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processors.rest.igfs.config.SystemConfig;
import org.apache.ignite.internal.processors.rest.igfs.model.S3Object;
import org.apache.ignite.internal.processors.rest.igfs.model.S3ObjectInputStream;
import org.apache.ignite.internal.processors.rest.igfs.model.StringInputStream;
import org.apache.ignite.internal.processors.rest.igfs.service.S3Service;
import org.apache.ignite.internal.processors.rest.igfs.service.Impl.S3IgfsServiceImpl;
import org.apache.ignite.internal.processors.rest.igfs.service.Impl.S3LocalFileServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import cn.hutool.core.io.FileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.mail.internet.MimeUtility;


/**
 * for amis
 */
@RestController
@RequestMapping(value = "/docs")
@Blocking
@CrossOrigin
public class JsonObjectController extends VertxInstanceAware{
	private static final String ACCEPT_JSON = "Accept=application/json";
	private static final String HEADER_X_AMZ_META_PREFIX = "x-amz-meta-";
	
	@Autowired
    @Qualifier("systemConfig")
    private SystemConfig systemConfig;
	
	private S3Service s3Service;

	private String bucketName = "json_datasets";
	
	private S3Service s3Service() {
    	String region = this.getIgniteInstanceName();    	
    	if(s3Service==null) {
    		try {
    			Ignite ignite = Ignition.ignite(region);
    			s3Service = new S3IgfsServiceImpl(region,systemConfig);
    		}
    		catch(Exception e) {
    			s3Service = new S3LocalFileServiceImpl(region,systemConfig);
    		}
    	}
    	return s3Service;
    }

	private String key(String coll, String docId) {
		coll = StringUtils.trimLeadingCharacter(coll, '/');
		coll = StringUtils.trimTrailingCharacter(coll, '/');
		docId = StringUtils.trimLeadingCharacter(docId, '/');
		docId = StringUtils.trimTrailingCharacter(docId, '/');

		return coll + "/" + docId + ".json";
	}
	
	private Map<String, String> getUserMetadata(final HttpServerRequest request) {
        return request.headers().entries().stream()
            .filter(header -> header.getKey().startsWith(HEADER_X_AMZ_META_PREFIX))
            .collect(Collectors.toMap(
                header -> header.getKey().substring(HEADER_X_AMZ_META_PREFIX.length()),
                header -> header.getValue()
            ));
      }

	/**
	 * 展示JSON collection列表
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET, headers = ACCEPT_JSON)
	public JsonObject list(@RequestParam(value = "name", required = false) String name) {
		JsonObject JsonObject = new JsonObject();
		try {
			// 需要显示的目录路径
			// 返回的结果集
			List<JsonObject> fileItems = new ArrayList<>();

			List<S3Object> list = s3Service().listObjects(bucketName, null);

			String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
			SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
			for (S3Object pathObj : list) {
				String fname = pathObj.getKey();

				if (name != null && !name.isEmpty()) {
					if (name.charAt(0) == '/' && !fname.startsWith(name)) {
						continue;
					}
					if (name.charAt(0) != '/' && fname.indexOf(name) < 0) {
						continue;
					}
				}

				// 封装返回JSON数据
				JsonObject fileItem = new JsonObject();
				fileItem.put("name", fname);
				fileItem.put("date", dt.format(pathObj.getMetadata().getLastModified()));
				fileItem.put("size", pathObj.getMetadata().getContentLength());
				fileItem.put("etag", pathObj.getMetadata().getETag());
				fileItem.put("type", fname.endsWith("/") ? "dir" : "file");
				fileItems.add(fileItem);
			}

			JsonObject.put("data", fileItems);
			JsonObject.put("status", 0);
			return JsonObject;
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	/**
	 * 展示JSON对象列表
	 */
	@RequestMapping(value = "/:collection", method = RequestMethod.GET, headers = ACCEPT_JSON)
	public JsonObject all_collection_docs(@PathVariable("collection") String collection,
			@RequestParam(value = "name", required = false) String name) {
		JsonObject JsonObject = new JsonObject();
		try {
			// 需要显示的目录路径
			// 返回的结果集

			List<S3Object> list = s3Service().listObjects(bucketName, collection);

			List<JsonObject> fileItems = list.parallelStream().filter((S3Object pathObj) -> {
				String fname = pathObj.getKey().substring(collection.length());

				if (name != null && !name.isEmpty()) {
					if (name.charAt(0) == '/' && !fname.startsWith(name)) {
						return false;
					}
					if (name.charAt(0) != '/' && fname.indexOf(name) < 0) {
						return false;
					}
				}
				return true;
			}).map((S3Object pathObj) -> {
				String fname = pathObj.getKey();

				S3ObjectInputStream objectStream = s3Service().getObject(bucketName, fname);
				
				try {
					
					JsonObject json = new JsonObject(Buffer.buffer(objectStream.readAllBytes()));
					if (fname.endsWith(".json")) {
						fname = fname.substring(0, fname.length() - 5);
					}
					json.put("_id", fname);
					return json;

				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}

			}).collect(Collectors.toList());

			JsonObject.put("data", fileItems);
			JsonObject.put("status", 0);
			return JsonObject;
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	/**
	 * 文档创建
	 */
	@RequestMapping(value = "/:collection/:path", method = RequestMethod.POST, headers = ACCEPT_JSON)
	public JsonObject upload(@PathVariable("collection") String collection, 
			@PathVariable("path") String destination,
			HttpServerRequest request,
			@RequestBody JsonObject json) {

		try {
			Map<String,String> userMeta = getUserMetadata(request);
			StringInputStream in = new StringInputStream(json.encode());
			s3Service().putObject(bucketName, key(collection, destination), in, userMeta);
			return success(key(collection, destination));
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	/**
	 * 文档下载/预览
	 * 
	 * @throws IOException
	 */
	@RequestMapping(value = "/:collection/:path", method = RequestMethod.GET, headers = ACCEPT_JSON)
	public void preview(HttpServerResponse response, 
			@PathVariable("collection") String collection,
			@PathVariable("path") String path) throws IOException {

		response.putHeader("Content-Type","application/json");
		response.putHeader("Content-Disposition",
				"inline; filename=\"" + MimeUtility.encodeWord(FileUtil.getName(path)) + "\"");

		try (S3ObjectInputStream objectStream = s3Service().getObject(bucketName, key(collection, path))) {
			
			response.end(Buffer.buffer(objectStream.readAllBytes()));
		} catch (Exception e) {
			response.setStatusCode(HttpStatus.NOT_FOUND.value());
			response.setStatusMessage("Resource Not Found");
		}
	}

	/**
	 * 文件全量更新，返回新版本
	 */
	@RequestMapping(value = "/:collection/:path", method = RequestMethod.PUT, headers = ACCEPT_JSON)
	@ResponseBody
	public JsonObject put(@PathVariable("collection") String collection, 
			@PathVariable("path") String destination,
			HttpServerRequest request,
			@RequestBody JsonObject updates) {

		try {
			Map<String,String> userMeta = getUserMetadata(request);
			StringInputStream in = new StringInputStream(updates.encode());
			s3Service().putObject(bucketName, key(collection, destination), in, userMeta);
			return success(key(collection, destination));
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}

	}

	/**
	 * 删除文档中的内容keys
	 */
	@RequestMapping(value = "/:collection/:path", method = RequestMethod.DELETE, headers = ACCEPT_JSON)
	@ResponseBody
	public JsonObject remove(@PathVariable("collection") String collection, 
			@PathVariable("path") String destination,
			@RequestBody JsonObject deletes) {
		try {
			String path = key(collection, destination);
			if (deletes == null) {
				s3Service().deleteObject(bucketName, path);
				return success(path);
			}

			S3ObjectInputStream objectStream = s3Service().getObject(bucketName, path);
			
			JsonObject json = new JsonObject(Buffer.buffer(objectStream.readAllBytes()));
			
			List<String> keys = deleteMap(json.getMap(), deletes.getMap());			

			StringInputStream in = new StringInputStream(json.encode());


			s3Service().putObject(bucketName, path, in, null);
			return success(keys);

		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	private List<String> deleteMap(Map<String, Object> json, Map<String, Object> deletes) {
		List<String> removekeys = new ArrayList<>();
		for (Map.Entry<String, Object> ent : deletes.entrySet()) {
			String key = ent.getKey();
			if (ent.getValue() instanceof Map) {
				Object value = json.get(key);
				if (value instanceof Map) {
					List<String> subRemovekeys = deleteMap((Map) value, (Map) ent.getValue());
					for(String sub: subRemovekeys) {
						removekeys.add(key+"."+sub);
					}
				}
			} else if (ent.getValue() instanceof List) {
				Object value = json.get(key);
				if (value instanceof Map) {
					Map<String, Object> jsonValue = (Map) value;
					List keys = (List) ent.getValue();
					for (Object id : keys) {
						if(jsonValue.remove(id)!=null) {
							removekeys.add(id.toString());
						}
					}
				}
			} else {
				if(json.remove(key)!=null)
					removekeys.add(key);
			}
		}
		return removekeys;
	}	
	

	/**
	 * 查看文件元信息
	 */
	@RequestMapping("/:collection/:path/meta")
	public JsonObject getContent(@PathVariable("collection") String collection,
			@PathVariable("path") String destination) {
		try {
			JsonObject JsonObject = new JsonObject();
			List<S3Object> list = s3Service().listObjects(bucketName, key(collection, destination));
			String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
			SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
			for (S3Object pathObj : list) {
				String fname = pathObj.getKey();

				// 封装返回JSON数据
				JsonObject fileItem = new JsonObject();
				fileItem.put("name", fname);
				fileItem.put("date", dt.format(pathObj.getMetadata().getLastModified()));
				fileItem.put("size", pathObj.getMetadata().getContentLength());
				fileItem.put("etag", pathObj.getMetadata().getETag());
				fileItem.put("type", fname.endsWith("/") ? "dir" : "file");

				JsonObject.put("data", fileItem);
				JsonObject.put("status", 0);
			}

			if (list.size() == 0) {
				JsonObject.put("status", 400);
			}

			return JsonObject;

		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	@RequestMapping(value = "/:collection/:path", method = RequestMethod.PATCH, headers = ACCEPT_JSON)
	public JsonObject patch(@PathVariable("collection") String collection, 
			@PathVariable("path") String path,
			@RequestBody JsonObject updates) {

		try {
			S3ObjectInputStream objectStream = s3Service().getObject(bucketName, path);

			JsonObject json = new JsonObject(Buffer.buffer(objectStream.readAllBytes()));
			
			updates.forEach((entry)->{
				json.put(entry.getKey(),entry.getValue());
			});
			

			StringInputStream in = new StringInputStream(json.encode());

			s3Service().putObject(bucketName, key(collection, path), in, null);
			return success("");
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}

	}

	private JsonObject error(String msg, int status) {
		JsonObject result = new JsonObject();
		result.put("message", msg);
		result.put("status", status);
		result.put("error", msg);
		return result;

	}

	private JsonObject success(Object data) {
		JsonObject result = new JsonObject();
		result.put("message", "success");
		result.put("status", 0);
		result.put("data", data);
		return result;
	}

}
