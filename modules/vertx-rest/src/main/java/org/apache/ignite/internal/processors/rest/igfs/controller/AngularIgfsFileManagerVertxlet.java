package org.apache.ignite.internal.processors.rest.igfs.controller;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.activation.MimetypesFileTypeMap;
import javax.mail.internet.MimeUtility;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;

import org.apache.ignite.internal.processors.rest.igfs.config.SystemConfig;
import org.apache.ignite.internal.processors.rest.igfs.util.DateUtil;
import org.apache.ignite.internal.processors.rest.igfs.util.IgfsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;

import cn.hutool.core.io.FileUtil;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.Vertxlet;
import io.vertx.webmvc.annotation.VertxletMapping;
import io.vertx.webmvc.common.VertxletException;


/**
 * This servlet serve angular-filemanager call<br>
 * It's here for example purpouse, to use it you have to put it in your java web
 * project<br>
 * Put in web.xml the servlet mapping
 *
 *
 * that catch all request to path /fm/*<br>
 * in angular-filemanager-master/index.html uncomment links to js files<br>
 * in my assest/config.js I have :
 *
 * <pre>
 * listUrl : "/fm/listUrl",
 * uploadUrl : "/fm/uploadUrl",
 * renameUrl : "/fm/renameUrl",
 * copyUrl : "/fm/copyUrl",
 * removeUrl : "/fm/removeUrl",
 * editUrl : "/fm/editUrl",
 * getContentUrl : "/fm/getContentUrl",
 * createFolderUrl : "/fm/createFolderUrl",
 * downloadFileUrl : "/fm/downloadFileUrl",
 * compressUrl : "/fm/compressUrl",
 * extractUrl : "/fm/extractUrl",
 * permissionsUrl : "/fm/permissionsUrl",
 * </pre>
 *
 * During initialization this servlet load some config properties from a file
 * called angular-filemanager.properties in your classes folder. You can set
 * repository.base.url and date.format <br>
 * Default values are : repository.base.url = "" and date.format = "yyyy-MM-dd
 * hh:mm:ss" (Wed, 4 Jul 2001 12:08:56) <br>
 * <br>
 * <b>NOTE:</b><br>
 * Does NOT manage 'preview' parameter in download<br>
 * Compress and expand are NOT implemented<br>
 *
 * @author Paolo Biavati https://github.com/paolobiavati
 */
@VertxletMapping(url="/filemanager/file*")
public class AngularIgfsFileManagerVertxlet extends Vertxlet {

    private static final Logger LOG = LoggerFactory.getLogger(AngularIgfsFileManagerVertxlet.class);

    private static final long serialVersionUID = -8453502699403909016L;
    
    private static final String contextPath = "/filemanager/file";    


    private Map<Mode, Boolean> enabledAction = new HashMap<>();

    enum Mode {

        list, rename, move, copy, remove, edit, getContent, createFolder, changePermissions, compress, extract, upload
    }

    private String REPOSITORY_BASE_PATH = "/tmp";
    private String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss"; // (2001-07-04 12:08:56)
   
    private Map<String,IgniteFileSystem> fsMap = new HashMap<>();


    @Override
    public void init() throws VertxletException {        
        
        String tmp = System.getProperty("ava.io.tmpdir");
        if(tmp!=null) {
        	REPOSITORY_BASE_PATH = tmp;
        }
        String configValue = getInitParameter("date.format");
        if (configValue != null) {
        	SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);            
            DATE_FORMAT = configValue;
        }
        if (getInitParameter("enabled.action") == null) {
        	enabledAction.put(Mode.rename, true);
            enabledAction.put(Mode.move, true);
            enabledAction.put(Mode.remove, true);
            enabledAction.put(Mode.edit, true);
            enabledAction.put(Mode.createFolder, true);
            enabledAction.put(Mode.changePermissions, true);
            enabledAction.put(Mode.compress, true);
            enabledAction.put(Mode.extract, true);
            enabledAction.put(Mode.copy, true);
            enabledAction.put(Mode.upload, true);
        } else {
            final String enabledActions = getInitParameter("enabled.action").toLowerCase();
            Pattern movePattern = Pattern.compile("\\bmove\\b");
            
            enabledAction.put(Mode.rename, enabledActions.contains("rename"));
            enabledAction.put(Mode.move, movePattern.matcher(enabledActions).find());
            enabledAction.put(Mode.remove, enabledActions.contains("remove"));
            enabledAction.put(Mode.edit, enabledActions.contains("edit"));
            enabledAction.put(Mode.createFolder, enabledActions.contains("createfolder"));
            enabledAction.put(Mode.changePermissions, enabledActions.contains("changepermissions"));
            enabledAction.put(Mode.compress, enabledActions.contains("compress"));
            enabledAction.put(Mode.extract, enabledActions.contains("extract"));
            enabledAction.put(Mode.copy, enabledActions.contains("copy"));
            enabledAction.put(Mode.upload, enabledActions.contains("upload"));
        }

    }
    
    /**
     *  获取fs，使用单一的fs存储所有的buckets
     * @param bucketName
     * @return
     */
    protected IgniteFileSystem fs(String bucketName) {    		
    	IgniteFileSystem igfs = allFS().get(bucketName);
        return igfs;

    }
    
    protected Map<String,IgniteFileSystem> allFS(){
    	if(fsMap.isEmpty()) {
    		for(Ignite ignite: Ignition.allGrids()) {
    			for(IgniteFileSystem fs:ignite.fileSystems()) {
    				String prefix = !StringUtils.hasText(ignite.name()) ? fs.name(): ignite.name()+"-"+fs.name();
    				if(!prefix.isBlank()) {
    					fsMap.put(prefix, fs);
    				}
    			}
    		}
    	}
    	return fsMap;
    }

	/**
	 *
	 * @param webjarsResourceURI
	 * @return
	 */
	private String[] getFileToken(String webjarsResourceURI) {
		while (webjarsResourceURI.startsWith("/")) {
			webjarsResourceURI = webjarsResourceURI.substring(1);
		}
		String[] tokens = webjarsResourceURI.split("/", 2);
		if (tokens.length == 1) {
			return new String[] { tokens[0], "/" };
		}
		tokens[1] = "/" + tokens[1];
		return tokens;
	}

	private IgfsFile getFile(String webjarsResourceURI) {
		while (webjarsResourceURI.startsWith("/")) {
			webjarsResourceURI = webjarsResourceURI.substring(1);
		}
		String[] tokens = webjarsResourceURI.split("/", 2);
		IgniteFileSystem fs = fs(tokens[0]);
		if (tokens.length == 1) {
			return fs.info(new IgfsPath("/"));
		}
		return fs.info(new IgfsPath("/" + tokens[1]));
	}

	private IgfsPath getPath(String webjarsResourceURI) {
		while (webjarsResourceURI.startsWith("/")) {
			webjarsResourceURI = webjarsResourceURI.substring(1);
		}
		String[] tokens = webjarsResourceURI.split("/", 2);
		if (tokens.length == 1) {
			return new IgfsPath("/");
		}
		return new IgfsPath("/" + tokens[1]);
	}
   
    @Override
    public void doGet(HttpServerRequest request, HttpServerResponse response) throws VertxletException,IOException {
    	     
        
    	String action = request.getParam("action");
    	String pathName = request.getParam("path");
    	// Catch download requests    	
    	if(action==null) { // view
    		action = "download";
    		String uri = request.uri();
    		pathName = uri.replaceFirst(contextPath, "");
    	}
    	
    	pathName = URLDecoder.decode(pathName,"UTF-8");

        // [$config.downloadFileUrl]?mode=download&preview=true&path=/public_html/image.jpg
        String[] tokens = getFileToken(pathName);
        IgniteFileSystem fs = fs(tokens[0]);
        pathName = tokens[1];
        if (Mode.list.name().equals(action)) {
        	JsonObject params = new JsonObject();
        	request.params().forEach((k,v)->{
        		params.put(k, v);
        	});
        	
        	JsonObject responseJsonObject = list(params);
        	setContentType(response,"application/json;charset=UTF-8");        
            response.end(responseJsonObject.encodePrettily());
        }
        else if (Mode.getContent.name().equals(action)) {
        	JsonObject params = new JsonObject();
        	request.params().forEach((k,v)->{
        		params.put(k, v);
        	});
        	
        	JsonObject responseJsonObject = getContent(params);
        	setContentType(response,"application/json;charset=UTF-8");        
            response.end(responseJsonObject.encodePrettily());
        }
        else if ("download".equals(action)) {
        	IgfsPath fspath = new IgfsPath(pathName);
            IgfsFile file = fs.info(fspath);
            if (file==null) {
                // if not a file, it is a folder, show this error.  
                response.setStatusCode(HttpStatus.NOT_FOUND.value()).end("Resource Not Found");
                return;
            }
            if (!file.isFile()) {
                // if not a file, it is a folder, show this error.  
                response.setStatusCode(HttpStatus.NOT_FOUND.value()).end("Resource Is isDirectory");
                return;
            }

            //获取mimeType
            String mimeType = new MimetypesFileTypeMap().getContentType(fspath.name());
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            setContentType(response,mimeType);
            response.putHeader("Content-Disposition", "inline; filename=\"" + MimeUtility.encodeWord(fspath.name()) + "\"");
            
            
            try {
            	OutputStream output = getOutputStream(response);
            	IgfsUtils.pipe(fs, fspath, output);
            	output.close();
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
                throw ex;
            }
        } 
        else if ("downloadMultiple".equals(action)) {
        	
            String toFilename = request.getParam("toFilename");
            List<String> items = request.params().getAll("items");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(baos))) {
                for (String item : items) {
                	IgfsPath fspath = new IgfsPath(item);
                	IgfsFile file = fs.info(fspath);                   
                    if (file.isFile()) {
                        ZipEntry zipEntry = new ZipEntry(fspath.name());
                        zos.putNextEntry(zipEntry);                       
                        try {
                        	IgfsUtils.pipe(fs, fspath, zos);                       
	                    } catch (IOException ex) {
	                        LOG.error(ex.getMessage(), ex);
	                        throw ex;
	                    }
                        finally {
                            zos.closeEntry();
                        }
                    }
                }
            }
            setContentType(response,"application/zip");
            response.putHeader("Content-Disposition", "inline; filename=\"" + MimeUtility.encodeWord(toFilename) + "\"");
            OutputStream output = getOutputStream(response);
            output.write(baos.toByteArray());
            output.close();
        }
    }

    @Override
    protected void doPost(RoutingContext rc) throws VertxletException, IOException {
    	HttpServerRequest request = rc.request();
    	HttpServerResponse response = rc.response();
        try {        
        	
            // if request contains multipart-form-data
            if (isMultipartContent(request)) {
                if (isSupportFeature(Mode.upload)) {
                    uploadFile(rc, request, response);
                } else {
                    setError(new IllegalAccessError(notSupportFeature(Mode.upload).getString("error")), response);
                }
            } // all other post request has jspn params in body
            else {
                fileOperation(rc, request, response);
            }            
            
        } catch (VertxletException | IOException ex) {
            LOG.error(ex.getMessage(), ex);
            setError(ex, response);
        }

    }

    private boolean isSupportFeature(Mode mode) {
        LOG.debug("check spport {}", mode);
        return Boolean.TRUE.equals(enabledAction.get(mode));
    }

    private JsonObject notSupportFeature(Mode mode) throws VertxletException {
        return error("This implementation not support " + mode + " feature");
    }

    private void setError(Throwable t, HttpServerResponse response) {
        try {
            // { "result": { "success": false, "error": "message" } }
            JsonObject responseJsonObject = error(t.getMessage());
            setContentType(response,"application/json;charset=UTF-8");
            
            response.end(responseJsonObject.encodePrettily());
           
        } catch (Exception ex) {
            response.setStatusCode(500);
            response.end(ex.getMessage());
        }

    }

    private void uploadFile(RoutingContext rc, HttpServerRequest request, HttpServerResponse response) throws VertxletException,IOException {
        // URL: $config.uploadUrl, Method: POST, Content-Type: multipart/form-data
        // Unlimited file upload, each item will be enumerated as file-1, file-2, etc.
        // [$config.uploadUrl]?destination=/public_html/image.jpg&file-1={..}&file-2={...}
        if (isSupportFeature(Mode.upload)) {
            LOG.debug("upload now");
            try {
                String destination = request.getParam("destination");
                String[] tokens = getFileToken(destination);
                IgniteFileSystem fs = fs(tokens[0]);
                String pathName = tokens[1];
                if(pathName.equals("/"))
                	pathName = "";

                
                List<FileUpload> fileUploads = rc.fileUploads();
                if (fileUploads != null && !fileUploads.isEmpty()) {
                  // 遍历上传的文件
                  for (FileUpload fileEntry : fileUploads) {
                    String uploadedFileName = fileEntry.uploadedFileName();
                    System.out.println("文件已上传：" + uploadedFileName);
                    
                    IgfsPath igpath = new IgfsPath(pathName+"/"+ fileEntry.fileName());
                    File tempFile = new File(uploadedFileName);
                    FileInputStream in = new FileInputStream(tempFile);
                    long n = IgfsUtils.create(fs, igpath, in);
                    if (n<0) {
                        LOG.debug("write error");
                        throw new Exception("write error");
                    }
                  }
                  
                  JsonObject responseJsonObject = this.success(null);
                  setContentType(response,"application/json;charset=UTF-8");                   
                  response.end(responseJsonObject.encodePrettily());
                  
                } else {
                  response.putHeader("Content-Type", "text/plain").end("没有文件上传");
                  LOG.debug("file size  = 0");
                  throw new VertxletException(HttpStatus.BAD_REQUEST.value(),"file size  = 0");
                }                

            } catch (IOException e) {
                LOG.error("Cannot parse multipart request: item.getInputStream");
                throw new VertxletException("Cannot parse multipart request: item.getInputStream", e);
            } catch (Exception e) {
                LOG.error("Cannot write file", e);
                throw new VertxletException("Cannot write file", e);
            }
        } else {
            throw new VertxletException(HttpStatus.FORBIDDEN.value(),notSupportFeature(Mode.upload).getString("error"));
        }
    }
   

    private void fileOperation(RoutingContext rc, HttpServerRequest request, HttpServerResponse response) throws VertxletException, IOException {
        JsonObject responseJsonObject = null;
        try {           
            JsonObject params = rc.body().asJsonObject();
            // legge mode e chiama il metodo aapropriato
            Mode mode = Mode.valueOf(params.getString("action"));
            switch (mode) {
                case createFolder:
                    responseJsonObject = isSupportFeature(mode) ? createFolder(params) : notSupportFeature(mode);
                    break;
                case changePermissions:
                    responseJsonObject = isSupportFeature(mode) ? changePermissions(params) : notSupportFeature(mode);
                    break;
                case compress:
                    responseJsonObject = isSupportFeature(mode) ? compress(params) : notSupportFeature(mode);
                    break;
                case copy:
                    responseJsonObject = isSupportFeature(mode) ? copy(params) : notSupportFeature(mode);
                    break;
                case remove:
                    responseJsonObject = isSupportFeature(mode) ? remove(params) : notSupportFeature(mode);
                    break;
                case getContent:
                    responseJsonObject = getContent(params);
                    break;
                case edit: // get content
                    responseJsonObject = isSupportFeature(mode) ? editFile(params) : notSupportFeature(mode);
                    break;
                case extract:
                    responseJsonObject = isSupportFeature(mode) ? extract(params) : notSupportFeature(mode);
                    break;
                case list:
                    responseJsonObject = list(params);
                    break;
                case rename:
                    responseJsonObject = isSupportFeature(mode) ? rename(params) : notSupportFeature(mode);
                    break;
                case move:
                    responseJsonObject = isSupportFeature(mode) ? move(params) : notSupportFeature(mode);
                    break;
                default:
                    throw new VertxletException(HttpStatus.NOT_IMPLEMENTED.value(),"not implemented");
            }
            if (responseJsonObject == null) {
                responseJsonObject = error("generic error : responseJsonObject is null");
            }
        } catch (IOException | VertxletException e) {
            responseJsonObject = error(e.getMessage());
        }
        setContentType(response,"application/json;charset=UTF-8");        
        response.end(responseJsonObject.encodePrettily());
   
    }

    private JsonObject list(JsonObject params) throws VertxletException,IOException {
        try {
            boolean onlyFolders = "true".equalsIgnoreCase(params.getString("onlyFolders"));
            String path = params.getString("path");
            LOG.debug("list path: Paths.get('{}', '{}'), onlyFolders: {}", REPOSITORY_BASE_PATH, path, onlyFolders);

            JsonArray resultList = new JsonArray();
            String[] tokens = getFileToken(path);            
            if(tokens[0].isEmpty()) {
            	// 默认的权限是自己可读写，其他人只能读
            	SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
                for(Map.Entry<String,IgniteFileSystem> fsEnt : allFS().entrySet()) {
    	            try {
    	            	IgniteFileSystem fs = fsEnt.getValue();
    	            	String fsName = fsEnt.getKey();    	                
    	                JsonObject el = new JsonObject();
	                    el.put("name", fsName);
	                    el.put("rights", getPermissions(null));
	                    el.put("date", dt.format(new Date(DateUtil.cpuStartTime)));
	                    el.put("size", fs.metrics().filesCount());
	                    el.put("type", "dir");
	                    resultList.add(el);
    	            } catch (IOException ex) {
    	            	ex.printStackTrace();
    	            }
                }
            }
            else {
            	IgniteFileSystem fs = fs(tokens[0]);
            	String pathName = tokens[1];
                IgfsPath igpath = new IgfsPath(pathName);
            	try {
	            	//-String fsName = "/" + fs.name() + "/";
	            	Collection<IgfsPath> directoryStream = fs.listPaths(igpath);
	                SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
	                // Calendar cal = Calendar.getInstance();
	                for (IgfsPath pathObj : directoryStream) {
	                	if(pathObj.name().startsWith(".")) {
	                		continue;
	                	}
	                	IgfsFile file = fs.info(pathObj);
	
	                    if (onlyFolders && !file.isDirectory()) {
	                        continue;
	                    }
	                    JsonObject el = new JsonObject();
	                    el.put("name", pathObj.name());
	                    el.put("rights", getPermissions(file));
	                    el.put("date", dt.format(new Date(file.modificationTime())));
	                    el.put("size", file.length());
	                    el.put("type", file.isDirectory() ? "dir" : "file");
	                    resultList.add(el);
	                }
	            } catch (IOException ex) {
	            	ex.printStackTrace();
	            }
            }
            
            JsonObject json = new JsonObject();
            json.put("result", resultList);
            return json;
        } catch (Exception e) {
            LOG.error("list:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JsonObject move(JsonObject params) throws VertxletException,IOException {
        try {
            JsonArray paths =  params.getJsonArray("items");            
            String[] tokens = getFileToken(params.getString("newPath"));
            IgniteFileSystem fs = fs(tokens[0]);
            String pathName = tokens[1];
            IgfsPath newpath = new IgfsPath(pathName);
            
            for (Object obj : paths) {
            	IgfsPath path = getPath(obj.toString());
                IgfsPath mpath = new IgfsPath(newpath + "/" + path.name());
                
                if (fs.exists(mpath)) {
                    return error(mpath.toString() + " already exits!");
                }
                LOG.debug("mv {} to {} exists? {}", path, mpath, fs.exists(mpath));
            }
            for (Object obj : paths) {
                IgfsPath path = getPath(obj.toString());
                IgfsPath mpath = new IgfsPath(newpath + "/" + path.name());
                IgfsUtils.move(fs, path, mpath, StandardCopyOption.REPLACE_EXISTING);
                
            }
            return success(params);
        } catch (IOException e) {
            LOG.error("move:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JsonObject rename(JsonObject params) throws VertxletException,IOException {
        try {
            String path = params.getString("item");
            String newpath = params.getString("newItemPath");
            LOG.debug("rename from: {} to: {}", path, newpath);
            String[] tokens = getFileToken(path);
            IgniteFileSystem fs = fs(tokens[0]);
            IgfsFile srcFile = getFile(path);
            IgfsPath destFile = getPath(newpath);
            if (srcFile.isFile()) {
            	IgfsUtils.move(fs, srcFile.path(), destFile, StandardCopyOption.ATOMIC_MOVE);
            } else {
            	IgfsUtils.move(fs, srcFile.path(), destFile, StandardCopyOption.ATOMIC_MOVE);
            }
            return success(params);
        } catch (IOException e) {
            LOG.error("rename:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JsonObject copy(JsonObject params) throws VertxletException,IOException {
        try {
            JsonArray paths = params.getJsonArray("items");
            String newpath = params.getString("newPath");
            String[] tokens = getFileToken(newpath);
            IgniteFileSystem fs = fs(tokens[0]);            
            IgfsPath destPath = getPath(newpath);            
            
            String newFileName = params.getString("singleFilename");
            for (Object obj : paths) {
            	// from file
            	String tofile = newFileName == null ? FileUtil.getName(obj.toString()) : newFileName;
            	IgfsPath mpath = new IgfsPath(destPath+"/"+tofile);
                LOG.debug("mv {} to {} exists? {}", obj, mpath, fs.exists(mpath));
                if (fs.exists(mpath)) {
                    return error(mpath.toString() + " already exits!");
                }
            }
            for (Object obj : paths) {
            	String tofile = newFileName == null ? FileUtil.getName(obj.toString()) : newFileName;
            	IgfsPath mpath = new IgfsPath(destPath+"/"+tofile);
            	IgfsPath path = getPath(obj.toString());
                IgfsUtils.copy(fs, path, mpath, StandardCopyOption.REPLACE_EXISTING);
            }
            return success(params);
        } catch (IOException e) {
            LOG.error("copy:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JsonObject remove(JsonObject params) throws VertxletException,IOException {
        JsonArray paths = params.getJsonArray("items");
        StringBuilder error = new StringBuilder();
        StringBuilder success = new StringBuilder();
        for (Object path : paths) {
        	String[] tokens = getFileToken(path.toString());
            IgniteFileSystem fs = fs(tokens[0]);
            String pathName = tokens[1];
            IgfsPath igpath = new IgfsPath(pathName);
            
            if (!IgfsUtils.delete(fs,igpath)) {
                error.append(error.length() > 0 ? "\n" : "Can't remove: \n/")
                        .append(path.toString());
            } else {
                success.append(error.length() > 0 ? "\n" : "\nBut remove remove: \n/")
                        .append(path.toString());
                LOG.debug("remove {}", path);
            }
        }
        if (error.length() > 0) {
            if (success.length() > 0) {
                success.append("\nPlease refresh this folder to list last result.");
            }
            throw new VertxletException(500,error.toString() + success.toString());
        } else {
            return success(params);
        }
    }

    private JsonObject getContent(JsonObject params) throws VertxletException,IOException {
        try {
            JsonObject json = new JsonObject();
            String[] tokens = getFileToken(params.getString("item"));
            IgniteFileSystem fs = fs(tokens[0]);
            String pathName = tokens[1];
            IgfsPath igpath = new IgfsPath(pathName);
            
            if(fs.info(igpath).length()>1024*1024*8) {
            	return error("file size is longer than 8M，can not get content use json!");
            }
            IgfsFile igfile = fs.info(igpath);
            if(igfile.isFile()) {
	            byte[] content = IgfsUtils.read(fs, igpath);
	            String text = new String(content,"UTF-8");
	            json.put("result", text);
	            
            }
            else {
            	JsonArray resultList = new JsonArray();
            	Collection<IgfsPath> directoryStream = fs.listPaths(igpath);
                SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
                // Calendar cal = Calendar.getInstance();
                for (IgfsPath pathObj : directoryStream) {
                	if(pathObj.name().startsWith(".")) {
                		continue;
                	}
                	IgfsFile file = fs.info(pathObj);
                    
                    JsonObject el = new JsonObject();
                    el.put("name", pathObj.name());
                    el.put("rights", getPermissions(file));
                    el.put("date", dt.format(new Date(file.modificationTime())));
                    el.put("size", file.length());
                    el.put("type", file.isDirectory() ? "dir" : "file");
                    resultList.add(el);
                }
                
                json.put("result", resultList);
            }
            json.put("meta", igfile.properties());
            return json;
        } catch (IOException ex) {
            LOG.error("getContent:" + ex.getMessage(), ex);
            return error(ex.getMessage());
        }
    }

    private JsonObject editFile(JsonObject params) throws VertxletException,IOException {
        // get content
        try {          
                   
            String content = params.getString("content");
            String[] tokens = getFileToken(params.getString("item"));
            IgniteFileSystem fs = fs(tokens[0]);
            String pathName = tokens[1];
            IgfsPath igpath = new IgfsPath(pathName);
            LOG.debug("editFile path: {}", igpath);    
            IgfsUtils.create(fs,igpath, content.getBytes("UTF-8"));
            return success(params);
        } catch (IOException e) {
            LOG.error("editFile:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JsonObject createFolder(JsonObject params) throws VertxletException,IOException {
        try {
            
            String[] tokens = getFileToken(params.getString("newPath"));
            IgniteFileSystem fs = fs(tokens[0]);
            String pathName = tokens[1];
            IgfsPath igpath = new IgfsPath(pathName);
            LOG.debug("createFolder path: {} name: {}", pathName);
            IgfsUtils.mkdirs(fs, igpath);
            return success(params);
        } catch (IgniteException e) {
            LOG.error("createFolder:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JsonObject changePermissions(JsonObject params) throws VertxletException {
        try {
            JsonArray paths = params.getJsonArray("items");
            String perms = params.getString("perms"); // "rw-r-x-wx"
            String permsCode = params.getString("permsCode"); // "653"
            boolean recursive = "true".equalsIgnoreCase(params.getString("recursive"));
            for (Object path : paths) {
                LOG.debug("changepermissions path: {} perms: {} permsCode: {} recursive: {}", path, perms, permsCode, recursive);
                
                String[] tokens = getFileToken(path.toString());
                IgniteFileSystem fs = fs(tokens[0]);
                String pathName = tokens[1];
                IgfsPath igpath = new IgfsPath(pathName);
                IgfsFile file = fs.info(igpath);
                int n = setPermissions(fs, file, perms, recursive);          
            }
            return success(params);
        } catch (IOException e) {
            LOG.error("changepermissions:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JsonObject compress(JsonObject params) throws VertxletException,IOException {
        try {
            JsonArray paths = params.getJsonArray("items");
            String paramDest = params.getString("destination");
            final Path dest = Paths.get(REPOSITORY_BASE_PATH, paramDest);
            Path zip = dest.resolve(params.getString("compressedFilename"));
            if (Files.exists(zip)) {
                return error(zip.toString() + " already exits!");
            }
            Map<String, String> env = new HashMap<>();
            env.put("create", "true");
            boolean zipped = false;
            try (FileSystem zipfs = FileSystems.newFileSystem(URI.create("jar:file:" + zip.toString()), env)) {
                for (Object path : paths) {
                    Path realPath = Paths.get(REPOSITORY_BASE_PATH, path.toString());
                    if (Files.isDirectory(realPath)) {
                        Files.walkFileTree(Paths.get(REPOSITORY_BASE_PATH, path.toString()), new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                                Files.createDirectories(zipfs.getPath(dir.toString().substring(dest.toString().length())));
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                Path pathInZipFile = zipfs.getPath(file.toString().substring(dest.toString().length()));
                                LOG.debug("compress: '{}'", pathInZipFile);
                                Files.copy(file, pathInZipFile, StandardCopyOption.REPLACE_EXISTING);
                                return FileVisitResult.CONTINUE;
                            }

                        });
                    } else {
                        Path pathInZipFile = zipfs.getPath("/", realPath.toString()
                                .substring(REPOSITORY_BASE_PATH.length() + paramDest.length()));
                        Path pathInZipFolder = pathInZipFile.getParent();
                        if (!Files.isDirectory(pathInZipFolder)) {
                            Files.createDirectories(pathInZipFolder);
                        }
                        LOG.debug("compress: '{}]", pathInZipFile);
                        Files.copy(realPath, pathInZipFile, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
                zipped = true;
            } finally {
                if (!zipped) {
                    Files.deleteIfExists(zip);
                }
            }
            return success(params);
        } catch (IOException e) {
            LOG.error("compress:" + e.getMessage(), e);
            return error(e.getClass().getSimpleName() + ":" + e.getMessage());
        }
    }

    private JsonObject extract(JsonObject params) throws VertxletException,IOException {
        boolean genFolder = false;
        Path dest = Paths.get(REPOSITORY_BASE_PATH, params.getString("destination"));
        final Path folder = dest.resolve(params.getString("folderName"));
        try {
            if (!Files.isDirectory(folder)) {
                genFolder = true;
                Files.createDirectories(folder);
            }
            String zip = params.getString("item");
            Map<String, String> env = new HashMap<>();
            env.put("create", "false");
            try (FileSystem zipfs = FileSystems.newFileSystem(URI.create("jar:file:" + Paths.get(REPOSITORY_BASE_PATH, zip).toString()), env)) {
                Files.walkFileTree(zipfs.getPath("/"), new SimpleFileVisitor<Path>() {

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (file.getNameCount() > 0) {
                            Path dest = folder.resolve(file.getNameCount() < 1 ? "" : file.subpath(0, file.getNameCount()).toString());
                            LOG.debug("extract {} to {}", file, dest);
                            try {
                                Files.copy(file, dest, StandardCopyOption.REPLACE_EXISTING);
                            } catch (Exception ex) {
                                LOG.error(ex.getMessage(), ex);
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        Path subFolder = folder.resolve(dir.getNameCount() < 1 ? "" : dir.subpath(0, dir.getNameCount()).toString());
                        if (!Files.exists(subFolder)) {
                            Files.createDirectories(subFolder);
                        }
                        return FileVisitResult.CONTINUE;
                    }

                });
            }
            return success(params);
        } catch (IOException e) {
            if (genFolder) {
                FileUtil.del(folder.toFile());
            }
            LOG.error("extract:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private String getPermissions(IgfsFile file) throws IOException {        
    	String perms = file!=null? file.property("permission", null): null;
    	if(perms!=null) {
    		return perms;
    	}
    	// default perms
        Set<PosixFilePermission> permissions = new HashSet<>();
        permissions.add(PosixFilePermission.OWNER_READ);
        permissions.add(PosixFilePermission.OWNER_WRITE);
        permissions.add(PosixFilePermission.OTHERS_READ);
        return PosixFilePermissions.toString(permissions);
    }    

    private int setPermissions(IgniteFileSystem fs, IgfsFile file, String permsCode, boolean recursive) throws IOException {
    	int n = 0;    	
    	if(recursive && file.isDirectory()) {  		
    		
    		Collection<IgfsPath> directoryStream = fs.listPaths(file.path());          
           
            for (IgfsPath pathObj : directoryStream) {
            	if(pathObj.name().startsWith(".")) {
            		continue;
            	}
            	IgfsFile sub_file = fs.info(pathObj);
            	n+=setPermissions(fs, sub_file, permsCode, recursive);               
            }	
    	}
    	n++;
    	HashMap<String,String> prop = new HashMap<>();
    	prop.put("permission", permsCode);
    	fs.update(file.path(), prop);
    	return n;
    }

    private JsonObject error(String msg) {
        // { "result": { "success": false, "error": "msg" } }
        JsonObject result = new JsonObject();
        result.put("success", false);
        result.put("error", msg);
        JsonObject json = new JsonObject();
        json.put("result", result);
        return json;
    }

    private JsonObject success(JsonObject params) {
        // { "result": { "success": true, "error": null } }
        JsonObject result = new JsonObject();
        result.put("success", true);
        result.put("error", null);
        
        JsonObject json = new JsonObject();
        json.put("result", result);
        json.put("params", params);
        return json;
    }

}
