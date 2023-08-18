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
import org.springframework.web.bind.annotation.*;

import java.io.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.mail.internet.MimeUtility;
import javax.servlet.http.HttpServletResponse;


/**
 * for amis 
 */
@RestController
@RequestMapping(value = "json")
public class JSONObjectController  {
	private static final String ACCEPT_JSON = "Accept=application/json";    
   
    @Autowired
    private S3Util s3Util;
    
    private String bucketName = "json_datasets";

    /**
     * 展示JSON对象列表
     */
    @RequestMapping(value="/list",method=RequestMethod.GET, headers=ACCEPT_JSON)
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
            	if(!fname.endsWith(".json")) {
            		continue;
            	}
            	
            	if(name!=null && !name.isEmpty()) {
            		if(fname.indexOf(name)<0) {
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
     * 文件创建
     */   
    @RequestMapping(value="/{path}",method=RequestMethod.POST, headers=ACCEPT_JSON)
    public JSONObject upload(@PathVariable("path") String destination,@RequestBody JSONObject json) {    

        try {
        	StringInputStream in = new StringInputStream(json.toJSONString());        	
            s3Util.upload(bucketName, destination, in);
            return success(destination);
        } catch (Exception e) {
            return error(e.getMessage(),500);
        }
    }
    
    
    /**
     * 文件下载/预览
     * @throws IOException 
     */
    @RequestMapping(value="/{path}",method=RequestMethod.GET, headers=ACCEPT_JSON)
    public void preview(HttpServletResponse response, @PathVariable("path") String path) throws IOException {

        response.setContentType("application/json");
        response.setHeader("Content-Disposition", "inline; filename=\"" + MimeUtility.encodeWord(FilenameUtils.getName(path)) + "\"");

        try (
        	InputStream in = s3Util.getFileInputStream(bucketName, path);
        	InputStream inputStream = new BufferedInputStream(in)) {
            FileCopyUtils.copy(inputStream, response.getOutputStream());
        }
        catch(Exception e) {
        	response.sendError(HttpServletResponse.SC_NOT_FOUND, "Resource Not Found");
        }
    }
    

    /**
     * 文件下载/预览
     */
    @RequestMapping(value="/{path}",method=RequestMethod.PUT, headers=ACCEPT_JSON)
    @ResponseBody
    public JSONObject put(@PathVariable("path") String path,@RequestParam("key") String key, @RequestBody JSONObject updates) {

        if (key==null) {
        	return error("Key Not Set",HttpServletResponse.SC_BAD_REQUEST);
        }      
            	
        try {
        	String jsonString = new String(s3Util.getFileByte(bucketName, path),"UTF-8");
            
            JSONObject json = JSONObject.parseObject(jsonString);
            json.put(key, updates);
            StringInputStream in = new StringInputStream(json.toJSONString());    
            
			s3Util.upload(bucketName, path, in);
			return success("");
		} catch (Exception e) {
			return error(e.getMessage(),500);
		}
        
    }

    /**
     * 删除文件或目录
     */
    @RequestMapping(value="/{path}",method=RequestMethod.DELETE, headers=ACCEPT_JSON)
    @ResponseBody
    public JSONObject remove(@PathVariable("path") String path,@RequestBody JSONObject deletes) {
        try {
        	
        	String jsonString = new String(s3Util.getFileByte(bucketName, path),"UTF-8");
            
            JSONObject json = JSONObject.parseObject(jsonString);
            for(String key: deletes.keySet()) {
            	json.remove(key);
            }
            StringInputStream in = new StringInputStream(json.toJSONString());        	
            
			s3Util.upload(bucketName, path, in);
			return success(deletes.keySet());
    		
            
        } catch (Exception e) {
            return error(e.getMessage(),500);
        }
    }

   

    /**
     * 查看文件内容,针对html、txt等可编辑文件
     */
    @RequestMapping("/getContent/{path}")
    public JSONObject getContent(@PathVariable("path") String path) {
        try {            
        	String jsonString = new String(s3Util.getFileByte(bucketName, path),"UTF-8");

            return success(jsonString);
            
        } catch (Exception e) {
            return error(e.getMessage(),500);
        }
    }

   

    @RequestMapping(value="/{path}",method=RequestMethod.PATCH, headers=ACCEPT_JSON)
    public JSONObject patch(@PathVariable("path") String path,@RequestBody JSONObject updates) {    
    	
        try {
        	String jsonString = new String(s3Util.getFileByte(bucketName, path),"UTF-8");
            
            JSONObject json = JSONObject.parseObject(jsonString);
            json.putAll(updates);
            StringInputStream in = new StringInputStream(json.toJSONString()); 
            
			s3Util.upload(bucketName, path, in);
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
