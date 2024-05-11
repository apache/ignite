package org.shaofan.s3.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.StringInputStream;

import org.apache.commons.io.FilenameUtils;
import org.shaofan.s3.util.S3Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;


import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.mail.internet.MimeUtility;
import javax.servlet.http.HttpServletResponse;


/**
 * for amis 
 */
@RestController
@RequestMapping(value = "docs")
@CrossOrigin
public class JSONObjectController  {
	private static final String ACCEPT_JSON = "Accept=application/json";    
   
    @Autowired
    private S3Util s3Util;
    
    private String bucketName = "json_datasets";
    
    private String key(String coll,String docId) {
    	coll = StringUtils.trimLeadingCharacter(coll,'/');
    	coll = StringUtils.trimTrailingCharacter(coll,'/');
    	docId = StringUtils.trimLeadingCharacter(docId, '/');
    	docId = StringUtils.trimTrailingCharacter(docId, '/');
    	
    	return coll+"/"+docId+".json";
    }

    /**
     * 展示JSON collection列表
     */
    @RequestMapping(value="/",method=RequestMethod.GET, headers=ACCEPT_JSON)
    public JSONObject list(@RequestParam(value="name",required=false) String name) {
    	JSONObject jsonObject = new JSONObject();
        try {
            // 需要显示的目录路径
            // 返回的结果集
            List<JSONObject> fileItems = new ArrayList<>();
            
            List<S3Object> list = s3Util.getObjectList(bucketName, null);

            String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
            SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
            for (S3Object pathObj : list) {
            	String fname = pathObj.key();            	
            	
            	if(name!=null && !name.isEmpty()) {
            		if(name.charAt(0)=='/' && !fname.startsWith(name)) {
            			continue;
            		}
            		if(name.charAt(0)!='/' && fname.indexOf(name)<0) {
            			continue;
            		}
            	}                   

                // 封装返回JSON数据
                JSONObject fileItem = new JSONObject();
                fileItem.put("name", fname);
                fileItem.put("date", dt.format(new Date(pathObj.lastModified().toEpochMilli())));
                fileItem.put("size", pathObj.size());
                fileItem.put("etag", pathObj.eTag());
                fileItem.put("type", fname.endsWith("/")?"dir":"file");
                fileItems.add(fileItem);
            }
            
            jsonObject.put("data", fileItems);
            jsonObject.put("status",0);
            return jsonObject;
        } catch (Exception e) {
            return error(e.getMessage(),500);
        }
    }
    

    /**
     * 展示JSON对象列表
     */
    @RequestMapping(value="/{collection}",method=RequestMethod.GET, headers=ACCEPT_JSON)
    public JSONObject all_collection_docs(@PathVariable("collection") String collection,@RequestParam(value="name",required=false) String name) {
    	JSONObject jsonObject = new JSONObject();
        try {
            // 需要显示的目录路径
            // 返回的结果集            
            
            List<S3Object> list = s3Util.getObjectList(bucketName, collection);

            List<JSONObject> fileItems = list.parallelStream().filter((S3Object pathObj)->{
            	String fname = pathObj.key().substring(collection.length());         	
            	
            	if(name!=null && !name.isEmpty()) {
            		if(name.charAt(0)=='/' && !fname.startsWith(name)) {
            			return false;
            		}
            		if(name.charAt(0)!='/' && fname.indexOf(name)<0) {
            			return false;
            		}            		
            	}
            	return true;
            }).map((S3Object pathObj)->{
            	String fname = pathObj.key();            	
            	String jsonString = new String(s3Util.getFileByte(bucketName, fname),StandardCharsets.UTF_8);
                
                JSONObject json = JSONObject.parseObject(jsonString);
                if(fname.endsWith(".json")) {
                	fname = fname.substring(0,fname.length()-5);
                }
                json.put("_id",fname);
            	return json;
            }).collect(Collectors.toList());
           
            jsonObject.put("data", fileItems);
            jsonObject.put("status",0);
            return jsonObject;
        } catch (Exception e) {
            return error(e.getMessage(),500);
        }
    }

    /**
     * 文档创建
     */   
    @RequestMapping(value="/{collection}/{path}",method=RequestMethod.POST, headers=ACCEPT_JSON)
    public JSONObject upload(@PathVariable("collection") String collection,@PathVariable("path") String destination,@RequestBody JSONObject json) {    

        try {
        	StringInputStream in = new StringInputStream(json.toJSONString());        	
            s3Util.upload(bucketName, key(collection,destination), in);
            return success(key(collection,destination));
        } catch (Exception e) {
            return error(e.getMessage(),500);
        }
    }
    
    
    /**
     *  文档下载/预览
     * @throws IOException 
     */
    @RequestMapping(value="/{collection}/{path}",method=RequestMethod.GET, headers=ACCEPT_JSON)
    public void preview(HttpServletResponse response,@PathVariable("collection") String collection, @PathVariable("path") String path) throws IOException {

        response.setContentType("application/json");
        response.setHeader("Content-Disposition", "inline; filename=\"" + MimeUtility.encodeWord(FilenameUtils.getName(path)) + "\"");

        try (
        	InputStream in = s3Util.getFileInputStream(bucketName, key(collection,path));
        	InputStream inputStream = new BufferedInputStream(in)) {
            FileCopyUtils.copy(inputStream, response.getOutputStream());
        }
        catch(Exception e) {
        	response.sendError(HttpServletResponse.SC_NOT_FOUND, "Resource Not Found");
        }
    }
    

    /**
     * 文件全量更新，返回新版本
     */
    @RequestMapping(value="/{collection}/{path}",method=RequestMethod.PUT, headers=ACCEPT_JSON)
    @ResponseBody
    public JSONObject put(@PathVariable("collection") String collection,@PathVariable("path") String destination,@RequestBody JSONObject updates) {
        	
        try {
        	
            StringInputStream in = new StringInputStream(updates.toJSONString());
            
			s3Util.upload(bucketName, key(collection,destination), in);
			return success(key(collection,destination));
		} catch (Exception e) {
			return error(e.getMessage(),500);
		}
        
    }

    /**
     * 删除文档中的内容keys
     */
    @RequestMapping(value="/{collection}/{path}",method=RequestMethod.DELETE, headers=ACCEPT_JSON)
    @ResponseBody
    public JSONObject remove(@PathVariable("collection") String collection,@PathVariable("path") String destination,@RequestBody JSONObject deletes) {
        try {
        	String path = key(collection,destination);
        	if(deletes==null) {
        		s3Util.delete(bucketName, path);
        		return success(path);
        	}
        	
        	String jsonString = new String(s3Util.getFileByte(bucketName, path),"UTF-8");
            
            JSONObject json = JSONObject.parseObject(jsonString);
            deleteMap(json,deletes);
            
            StringInputStream in = new StringInputStream(json.toJSONString());        	
            
			s3Util.upload(bucketName, path, in);
			return success(deletes.keySet());
            
        } catch (Exception e) {
            return error(e.getMessage(),500);
        }
    }

    private void deleteMap(Map<String,Object> json, Map<String,Object> deletes) {
    	for(Map.Entry<String,Object> ent: deletes.entrySet()) {
        	String key = ent.getKey();
        	if(ent.getValue() instanceof Map) {
        		Object value = json.get(key);
        		if(value instanceof Map) {
        			deleteMap((Map<String,Object>)value,(Map<String,Object>)ent.getValue());
        		}
        	}
        	else if(ent.getValue() instanceof List) {
        		Object value = json.get(key);
        		if(value instanceof Map) {
        			Map<String,Object> jsonValue = (Map<String,Object>)value;
        			List keys = (List)ent.getValue();
        			for(Object id: keys) {
        				jsonValue.remove(id);
        			}        			
        		}
        	}
        	else {
        		json.remove(key);
        	}
        }
    }
   

    /**
     * 查看文件内容,针对html、txt等可编辑文件
     */
    @RequestMapping("/{collection}/{path}/meta")
    public JSONObject getContent(@PathVariable("collection") String collection,@PathVariable("path") String destination) {
        try {      
        	JSONObject jsonObject = new JSONObject();
        	List<S3Object> list = s3Util.getObjectList(bucketName, key(collection,destination));
        	String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        	SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
        	for (S3Object pathObj : list) {
            	String fname = pathObj.key();            	        

                // 封装返回JSON数据
                JSONObject fileItem = new JSONObject();
                fileItem.put("name", fname);
                fileItem.put("date", dt.format(new Date(pathObj.lastModified().toEpochMilli())));
                fileItem.put("size", pathObj.size());
                fileItem.put("etag", pathObj.eTag());
                fileItem.put("type", fname.endsWith("/")?"dir":"file");
                jsonObject.put("data", fileItem);
                jsonObject.put("status",0);
            }
            if(list.size()!=1) {
            	jsonObject.put("status",400);
            }

            return jsonObject;
            
        } catch (Exception e) {
            return error(e.getMessage(),500);
        }
    }

   

    @RequestMapping(value="/{collection}/{path}",method=RequestMethod.PATCH, headers=ACCEPT_JSON)
    public JSONObject patch(@PathVariable("collection") String collection,@PathVariable("path") String destination,@RequestBody JSONObject updates) {    
    	
        try {
        	String jsonString = new String(s3Util.getFileByte(bucketName, key(collection,destination)),"UTF-8");
            
            JSONObject json = JSONObject.parseObject(jsonString);
            json.putAll(updates);
            StringInputStream in = new StringInputStream(json.toJSONString()); 
            
			s3Util.upload(bucketName, key(collection,destination), in);
			return success("");
		} catch (Exception e) {
			return error(e.getMessage(),500);
		}                    
        
    }

    private JSONObject error(String msg,int status) {     
        JSONObject result = new JSONObject();        
        result.put("message", msg);
        result.put("status", status);
        result.put("error", msg);
        return result;

    }

    private JSONObject success(Object data) {       
        JSONObject result = new JSONObject();
        result.put("message","success");
        result.put("status", 0);
        result.put("data", data);
        return result;
    }

}
