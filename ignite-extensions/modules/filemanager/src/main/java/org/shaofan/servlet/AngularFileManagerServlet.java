package org.shaofan.servlet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import jakarta.fileupload.FileItem;
import jakarta.fileupload.FileUploadException;
import jakarta.fileupload.disk.DiskFileItemFactory;
import jakarta.fileupload.servlet.ServletFileUpload;

/**
 * This servlet serve angular-filemanager call<br>
 * It's here for example purpouse, to use it you have to put it in your java web
 * project<br>
 * Put in web.xml the servlet mapping
 *
 * <pre>
 * &ltservlet&gt
 * 	&ltservlet-name&gtFileManagerServlet&lt/servlet-name&gt
 * 	&ltservlet-class&gtcom.project.web.servlet.AngularFileManagerServlet&lt/servlet-class&gt
 * &lt/servlet&gt
 * &ltservlet-mapping&gt
 * 	&ltservlet-name&gtFileManagerServlet&lt/servlet-name&gt
 * 	&lturl-pattern&gt/fm/*&lt/url-pattern&gt
 * &lt/servlet-mapping&gt
 * </pre>
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
public class AngularFileManagerServlet extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(AngularFileManagerServlet.class);

    private static final long serialVersionUID = -8453502699403909016L;

    private Map<Mode, Boolean> enabledAction = null;

    enum Mode {

        list, rename, move, copy, remove, edit, getContent, createFolder, changePermissions, compress, extract, upload
    }

    private String REPOSITORY_BASE_PATH = "/tmp";
    private String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss"; // (2001-07-04 12:08:56)
    //private String DATE_FORMAT = "EEE, d MMM yyyy HH:mm:ss z"; // (Wed, 4 Jul 2001 12:08:56)

    @Override
    public void init() throws ServletException {
        super.init();
        String configValue = this.getServletContext().getInitParameter("repository.base.path");
        REPOSITORY_BASE_PATH = configValue == null ? System.getProperty("ava.io.tmpdir")
                : configValue.trim();
        configValue = getInitParameter("date.format");
        if (configValue != null) {
            if (new SimpleDateFormat(DATE_FORMAT).format(new Date()) == null) {
                // Invalid date format
                LOG.error("throw invalid date.format");
                throw new ServletException("invalid date.format");
            }
            DATE_FORMAT = configValue;
        }
        if (getInitParameter("enabled.action") == null) {
            enabledAction = java.util.Collections.EMPTY_MAP;
        } else {
            final String enabledActions = getInitParameter("enabled.action").toLowerCase();
            Pattern movePattern = Pattern.compile("\\bmove\\b");
            enabledAction = new HashMap<>();
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
    *
    * @param webjarsResourceURI
    * @return
    */
   private String getFileName(String webjarsResourceURI) {
       String[] tokens = webjarsResourceURI.split("/");
       return tokens[tokens.length - 1];
   }

   
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	String action = request.getParameter("action");
    	String pathName = request.getParameter("path");
    	// Catch download requests    	
    	if(action==null) { //view
    		action = "download";
    		String uri = request.getRequestURI().replaceFirst(request.getContextPath(), "");  
    		pathName = uri.substring(6);
    	}
    	
    	pathName = URLDecoder.decode(pathName,"UTF-8");
        // [$config.downloadFileUrl]?mode=download&preview=true&path=/public_html/image.jpg
        
        if ("download".equals(action)) {
            
            File file = new File(REPOSITORY_BASE_PATH, pathName);
            if (!file.isFile()) {
                // if not a file, it is a folder, show this error.  
                response.sendError(HttpServletResponse.SC_NOT_FOUND, "Resource Not Found");
                return;
            }

            //获取mimeType
            String mimeType = new MimetypesFileTypeMap().getContentType(file.getName());
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            response.setContentType(mimeType);            
            //response.setHeader("Content-Type", "application/force-download");
            response.setHeader("Content-Disposition", "inline; filename=\"" + MimeUtility.encodeWord(file.getName()) + "\"");
            
            try (SeekableByteChannel channel = Files.newByteChannel(file.toPath())) {
                byte[] buffer = new byte[256 * 1024];
                ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
                for (int length = 0; (length = channel.read(byteBuffer)) != -1;) {
                    response.getOutputStream().write(buffer, 0, length);
                    byteBuffer.clear();
                }
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
                throw ex;
            } finally {
                response.getOutputStream().flush();
            }
        } else if ("downloadMultiple".equals(action)) {
        	
            String toFilename = request.getParameter("toFilename");
            String[] items = request.getParameterValues("items[]");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(baos))) {
                for (String item : items) {
                    Path path = Paths.get(REPOSITORY_BASE_PATH, item);
                    if (Files.exists(path)) {
                        ZipEntry zipEntry = new ZipEntry(path.getFileName().toString());
                        zos.putNextEntry(zipEntry);
                        byte buffer[] = new byte[2048];
                        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(path))) {
                            int bytesRead = 0;
                            while ((bytesRead = bis.read(buffer)) != -1) {
                                zos.write(buffer, 0, bytesRead);
                            }
                        } finally {
                            zos.closeEntry();
                        }
                    }
                }
            }
            response.setContentType("application/zip");
            response.setHeader("Content-Disposition", "inline; filename=\"" + MimeUtility.encodeWord(toFilename) + "\"");
            BufferedOutputStream output = new BufferedOutputStream(response.getOutputStream());
            output.write(baos.toByteArray());
            output.flush();
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            // if request contains multipart-form-data
            if (ServletFileUpload.isMultipartContent(request)) {
                if (isSupportFeature(Mode.upload)) {
                    uploadFile(request, response);
                } else {
                    setError(new IllegalAccessError(notSupportFeature(Mode.upload).getString("error")), response);
                }
            } // all other post request has jspn params in body
            else {
                fileOperation(request, response);
            }
        } catch (ServletException | IOException ex) {
            LOG.error(ex.getMessage(), ex);
            setError(ex, response);
        }

    }

    private boolean isSupportFeature(Mode mode) {
        LOG.debug("check spport {}", mode);
        return Boolean.TRUE.equals(enabledAction.get(mode));
    }

    private JSONObject notSupportFeature(Mode mode) throws ServletException {
        return error("This implementation not support " + mode + " feature");
    }

    private void setError(Throwable t, HttpServletResponse response) throws IOException {
        try {
            // { "result": { "success": false, "error": "message" } }
            JSONObject responseJsonObject = error(t.getMessage());
            response.setContentType("application/json;charset=UTF-8");
            PrintWriter out = response.getWriter();
            out.print(responseJsonObject);
            out.flush();
        } catch (IOException ex) {
            response.sendError(500, ex.getMessage());
        }

    }

    private void uploadFile(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        // URL: $config.uploadUrl, Method: POST, Content-Type: multipart/form-data
        // Unlimited file upload, each item will be enumerated as file-1, file-2, etc.
        // [$config.uploadUrl]?destination=/public_html/image.jpg&file-1={..}&file-2={...}
        if (isSupportFeature(Mode.upload)) {
            LOG.debug("upload now");
            try {
                String destination = null;
                Map<String, InputStream> files = new HashMap<>();
                ServletFileUpload sfu = new ServletFileUpload(new DiskFileItemFactory());
                sfu.setHeaderEncoding("UTF-8");
                List<FileItem> items = sfu.parseRequest(request);
                for (FileItem item : items) {
                    if (item.isFormField()) {
                        // Process regular form field (input type="text|radio|checkbox|etc", select, etc).
                        if ("destination".equals(item.getFieldName())) {
                            destination = item.getString("UTF-8");
                        }
                    } else {
                        // Process form file field (input type="file").
                        files.put(item.getName(), item.getInputStream());
                    }
                }
                if (files.isEmpty()) {
                    LOG.debug("file size  = 0");
                    throw new Exception("file size  = 0");
                } else {
                    for (Map.Entry<String, InputStream> fileEntry : files.entrySet()) {
                        Path path = Paths.get(REPOSITORY_BASE_PATH + destination, fileEntry.getKey());
                        if (!write(fileEntry.getValue(), path)) {
                            LOG.debug("write error");
                            throw new Exception("write error");
                        }
                        fileEntry.getValue().close();
                    }

                    JSONObject responseJsonObject = null;
                    responseJsonObject = this.success(responseJsonObject);
                    response.setContentType("application/json;charset=UTF-8");
                    PrintWriter out = response.getWriter();
                    out.print(responseJsonObject);
                    out.flush();
                }
            } catch (FileUploadException e) {
                LOG.error("Cannot parse multipart request: DiskFileItemFactory.parseRequest", e);
                throw new ServletException("Cannot parse multipart request: DiskFileItemFactory.parseRequest", e);
            } catch (IOException e) {
                LOG.error("Cannot parse multipart request: item.getInputStream");
                throw new ServletException("Cannot parse multipart request: item.getInputStream", e);
            } catch (Exception e) {
                LOG.error("Cannot write file", e);
                throw new ServletException("Cannot write file", e);
            }
        } else {
            throw new ServletException(notSupportFeature(Mode.upload).getString("error"));
        }
    }

    private boolean write(InputStream inputStream, Path path) {
        try {
            Files.copy(inputStream, path, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            return false;
        }
    }

    private void fileOperation(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        JSONObject responseJsonObject = null;
        try {
            // legge il parametro json
            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = request.getReader()) {
                String str;
                while ((str = br.readLine()) != null) {
                    sb.append(str);
                }
            }
            JSONObject params = (JSONObject) JSON.parse(sb.toString());
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
                    throw new ServletException("not implemented");
            }
            if (responseJsonObject == null) {
                responseJsonObject = error("generic error : responseJsonObject is null");
            }
        } catch (IOException | ServletException e) {
            responseJsonObject = error(e.getMessage());
        }
        response.setContentType("application/json;charset=UTF-8");
        PrintWriter out = response.getWriter();
        out.print(responseJsonObject);
        out.flush();
    }

    private JSONObject list(JSONObject params) throws ServletException {
        try {
            boolean onlyFolders = "true".equalsIgnoreCase(params.getString("onlyFolders"));
            String path = params.getString("path");
            LOG.debug("list path: Paths.get('{}', '{}'), onlyFolders: {}", REPOSITORY_BASE_PATH, path, onlyFolders);

            List<JSONObject> resultList = new ArrayList<>();
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(REPOSITORY_BASE_PATH, path))) {
                SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
                // Calendar cal = Calendar.getInstance();
                for (Path pathObj : directoryStream) {
                	if(pathObj.getFileName().toString().startsWith(".")) {
                		continue;
                	}
                    BasicFileAttributes attrs = Files.readAttributes(pathObj, BasicFileAttributes.class);

                    if (onlyFolders && !attrs.isDirectory()) {
                        continue;
                    }
                    JSONObject el = new JSONObject();
                    el.put("name", pathObj.getFileName().toString());
                    el.put("rights", getPermissions(pathObj));
                    el.put("date", dt.format(new Date(attrs.lastModifiedTime().toMillis())));
                    el.put("size", attrs.size());
                    el.put("type", attrs.isDirectory() ? "dir" : "file");
                    resultList.add(el);
                }
            } catch (IOException ex) {
            	ex.printStackTrace();
            }
            JSONObject json = new JSONObject();
            json.put("result", resultList);
            return json;
        } catch (Exception e) {
            LOG.error("list:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JSONObject move(JSONObject params) throws ServletException {
        try {
            JSONArray paths =  params.getJSONArray("items");
            Path newpath = Paths.get(REPOSITORY_BASE_PATH, params.getString("newPath"));
            for (Object obj : paths) {
                Path path = Paths.get(REPOSITORY_BASE_PATH, obj.toString());
                Path mpath = newpath.resolve(path.getFileName());
                LOG.debug("mv {} to {} exists? {}", path, mpath, Files.exists(mpath));
                if (Files.exists(mpath)) {
                    return error(mpath.toString() + " already exits!");
                }
            }
            for (Object obj : paths) {
                Path path = Paths.get(REPOSITORY_BASE_PATH, obj.toString());
                Path mpath = newpath.resolve(path.getFileName());
                Files.move(path, mpath, StandardCopyOption.REPLACE_EXISTING);
            }
            return success(params);
        } catch (IOException e) {
            LOG.error("move:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JSONObject rename(JSONObject params) throws ServletException {
        try {
            String path = params.getString("item");
            String newpath = params.getString("newItemPath");
            LOG.debug("rename from: {} to: {}", path, newpath);

            File srcFile = new File(REPOSITORY_BASE_PATH, path);
            File destFile = new File(REPOSITORY_BASE_PATH, newpath);
            if (srcFile.isFile()) {
                FileUtils.moveFile(srcFile, destFile);
            } else {
                FileUtils.moveDirectory(srcFile, destFile);
            }
            return success(params);
        } catch (IOException e) {
            LOG.error("rename:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JSONObject copy(JSONObject params) throws ServletException {
        try {
            JSONArray paths = ((JSONArray) params.get("items"));
            Path newpath = Paths.get(REPOSITORY_BASE_PATH, params.getString("newPath"));
            String newFileName = params.getString("singleFilename");
            for (Object obj : paths) {
                Path path = newFileName == null ? Paths.get(REPOSITORY_BASE_PATH,
                        obj.toString()) : Paths.get(".", newFileName);
                Path mpath = newpath.resolve(path.getFileName());
                LOG.debug("mv {} to {} exists? {}", path, mpath, Files.exists(mpath));
                if (Files.exists(mpath)) {
                    return error(mpath.toString() + " already exits!");
                }
            }
            for (Object obj : paths) {
                Path path = Paths.get(REPOSITORY_BASE_PATH, obj.toString());
                Path mpath = newpath.resolve(newFileName == null
                        ? path.getFileName() : Paths.get(".", newFileName).getFileName());
                Files.copy(path, mpath, StandardCopyOption.REPLACE_EXISTING);
            }
            return success(params);
        } catch (IOException e) {
            LOG.error("copy:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JSONObject remove(JSONObject params) throws ServletException {
        JSONArray paths = ((JSONArray) params.get("items"));
        StringBuilder error = new StringBuilder();
        StringBuilder success = new StringBuilder();
        for (Object obj : paths) {
            Path path = Paths.get(REPOSITORY_BASE_PATH, obj.toString());
            if (!FileUtils.deleteQuietly(path.toFile())) {
                error.append(error.length() > 0 ? "\n" : "Can't remove: \n/")
                        .append(path.subpath(1, path.getNameCount()).toString());
            } else {
                success.append(error.length() > 0 ? "\n" : "\nBut remove remove: \n/")
                        .append(path.subpath(1, path.getNameCount()).toString());
                LOG.debug("remove {}", path);
            }
        }
        if (error.length() > 0) {
            if (success.length() > 0) {
                success.append("\nPlease refresh this folder to list last result.");
            }
            throw new ServletException(error.toString() + success.toString());
        } else {
            return success(params);
        }
    }

    private JSONObject getContent(JSONObject params) throws ServletException {
        try {
            JSONObject json = new JSONObject();
            json.put("result", FileUtils.readFileToString(Paths.get(REPOSITORY_BASE_PATH,params.getString("item")).toFile(),"utf-8"));
            return json;
        } catch (IOException ex) {
            LOG.error("getContent:" + ex.getMessage(), ex);
            return error(ex.getMessage());
        }
    }

    private JSONObject editFile(JSONObject params) throws ServletException {
        // get content
        try {
            String path = params.getString("item");
            LOG.debug("editFile path: {}", path);

            File srcFile = new File(REPOSITORY_BASE_PATH, path);
            String content = params.getString("content");
            FileUtils.writeStringToFile(srcFile, content);
            return success(params);
        } catch (IOException e) {
            LOG.error("editFile:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JSONObject createFolder(JSONObject params) throws ServletException {
        try {
            Path path = Paths.get(REPOSITORY_BASE_PATH, params.getString("newPath"));
            LOG.debug("createFolder path: {} name: {}", path);
            Files.createDirectories(path);
            return success(params);
        } catch (FileAlreadyExistsException ex) {
            return success(params);
        } catch (IOException e) {
            LOG.error("createFolder:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JSONObject changePermissions(JSONObject params) throws ServletException {
        try {
            JSONArray paths = ((JSONArray) params.get("items"));
            String perms = params.getString("perms"); // "rw-r-x-wx"
            String permsCode = params.getString("permsCode"); // "653"
            boolean recursive = "true".equalsIgnoreCase(params.getString("recursive"));
            for (Object path : paths) {
                LOG.debug("changepermissions path: {} perms: {} permsCode: {} recursive: {}", path, perms, permsCode, recursive);
                File f = Paths.get(REPOSITORY_BASE_PATH, path.toString()).toFile();
                setPermissions(f, perms, recursive);
            }
            return success(params);
        } catch (IOException e) {
            LOG.error("changepermissions:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private JSONObject compress(JSONObject params) throws ServletException {
        try {
            JSONArray paths = ((JSONArray) params.get("items"));
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

    private JSONObject extract(JSONObject params) throws ServletException {
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
                FileUtils.deleteQuietly(folder.toFile());
            }
            LOG.error("extract:" + e.getMessage(), e);
            return error(e.getMessage());
        }
    }

    private String getPermissions(Path path) throws IOException {
        // http://www.programcreek.com/java-api-examples/index.php?api=java.nio.file.attribute.PosixFileAttributes
        PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if(fileAttributeView==null) {
        	Set<PosixFilePermission> permissions = new HashSet<>();
            permissions.add(PosixFilePermission.OWNER_READ);
            permissions.add(PosixFilePermission.OWNER_WRITE);
            permissions.add(PosixFilePermission.OTHERS_READ);
            return PosixFilePermissions.toString(permissions);
        }
        PosixFileAttributes readAttributes = fileAttributeView.readAttributes();
        Set<PosixFilePermission> permissions = readAttributes.permissions();
        return PosixFilePermissions.toString(permissions);
    }

    private String setPermissions(File file, String permsCode, boolean recursive) throws IOException {
        // http://www.programcreek.com/java-api-examples/index.php?api=java.nio.file.attribute.PosixFileAttributes
        PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class);
        fileAttributeView.setPermissions(PosixFilePermissions.fromString(permsCode));
        if (file.isDirectory() && recursive && file.listFiles() != null) {
            for (File f : file.listFiles()) {
                setPermissions(f, permsCode, recursive);
            }
        }
        return permsCode;
    }

    private JSONObject error(String msg) {
        // { "result": { "success": false, "error": "msg" } }
        JSONObject result = new JSONObject();
        result.put("success", false);
        result.put("error", msg);
        JSONObject json = new JSONObject();
        json.put("result", result);
        return json;
    }

    private JSONObject success(JSONObject params) {
        // { "result": { "success": true, "error": null } }
        JSONObject result = new JSONObject();
        result.put("success", true);
        result.put("error", null);
        JSONObject json = new JSONObject();
        json.put("result", result);
        return json;
    }

}
