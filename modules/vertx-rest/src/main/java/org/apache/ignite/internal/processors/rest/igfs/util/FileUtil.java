package org.apache.ignite.internal.processors.rest.igfs.util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.ignite.internal.processors.rest.igfs.model.Range;

public class FileUtil {

    public static boolean delete(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            //System.out.println("删除文件失败:" + fileName +"不存在！");
            return false;
        } else {
            if (file.isFile())
                return deleteFile(fileName);
            else
                return deleteDirectory(fileName);
        }
    }

    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public static boolean deleteDirectory(String dir) {
        if (!dir.endsWith("/"))
            dir = dir + "/";
        File dirFile = new File(dir);
        if ((!dirFile.exists()) || (!dirFile.isDirectory())) {
            return false;
        }
        boolean flag = true;
        File[] files = dirFile.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                flag = deleteFile(files[i].getAbsolutePath());
                if (!flag)
                    break;
            }
            else if (files[i].isDirectory()) {
                flag = deleteDirectory(files[i].getAbsolutePath());
                if (!flag)
                    break;
            }
        }
        if (!flag) {
            return false;
        }
        if (dirFile.delete()) {
            return true;
        } else {
            return false;
        }
    }

    public static String getCreationTime(File file) {
        if (file == null) {
            return null;
        }
        BasicFileAttributes attr = null;
        try {
            Path path =  file.toPath();
            attr = Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Instant instant = attr.creationTime().toInstant();
        String format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneId.systemDefault()).format(instant);
        return format;
    }

    public static Date getLastModifyTime(File file) {
        if (file == null) {
            return null;
        }
        BasicFileAttributes attr = null;
        try {
            Path path =  file.toPath();
            attr = Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Instant instant = attr.lastModifiedTime().toInstant();
        //String format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneId.systemDefault()).format(instant);
        return new Date(attr.lastModifiedTime().toMillis());
    }

    public static String getLastModifyTimeGMT(File file) {
        if (file == null) {
            return null;
        }
        Date modifiedTime = new Date(file.lastModified());
        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String time = dateFormat.format(modifiedTime);
        return time;
    }

    public static long getFileSize(File file) {
        if (file == null) {
            return 0;
        }
        BasicFileAttributes attr = null;
        try {
            Path path =  file.toPath();
            attr = Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  attr.size();
    }

    public static void saveFile(String filePath, InputStream inputStream){
        FileOutputStream out = null;
        try{
            out = new FileOutputStream(filePath);
            byte[] buffer = new byte[1024*16];
            int len = 0;            
            while ((len = inputStream.read(buffer)) != -1) {
            	out.write(buffer, 0, len);
            }            
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(out!=null){
               try{
                   out.flush();
                   out.close();
               }catch (Exception ex){

               }
            }
        }
    }
    
    public static String saveFileWithMd5(String filePath, InputStream inputStream){
        FileOutputStream out = null;        
        try{
        	//生成md5加密算法
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            
            out = new FileOutputStream(filePath);
            byte[] buffer = new byte[1024*16];
            int len = 0;            
            while ((len = inputStream.read(buffer)) != -1) {
            	out.write(buffer, 0, len);
            	md5.update(buffer, 0, len);
            }
            
            byte b[] = md5.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int j = 0; j < b.length; j++) {
            	String hex = Integer.toHexString(0xff & b[j]);  
                if (hex.length() == 1) 
                	buf.append('0');
                buf.append(hex);
            }
            String md5_32 = buf.toString();
            return md5_32;
            
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(out!=null){
               try{
                   out.flush();
                   out.close();
               }catch (Exception ex){

               }
            }
        }
        return null;
        
    }

    public static void saveFile(String filePath, byte[] data){
        FileOutputStream out = null;
        try{
            out = new FileOutputStream(filePath);
            out.write(data);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(out!=null){
                try{
                    out.flush();
                    out.close();
                }catch (Exception ex){

                }
            }
        }
    }

    public static FileInputStream getFileStream(String filePath) {    	
        try {
            File file = new File(filePath);
            FileInputStream fis = new FileInputStream(file);
            return fis;
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public static byte[] getFile(String filePath, long offset, int length) {
        byte[] buffer = null;
        try {
            File file = new File(filePath);
            FileInputStream fis = new FileInputStream(file);
            fis.skip(offset);
            buffer = fis.readNBytes(length);
            fis.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    public static byte[] convertStreamToByte(InputStream inputStream) throws IOException {
        byte[] buffer = new byte[1024*8];
        int len = 0;
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024*8);
        while ((len = inputStream.read(buffer)) != -1) {
            bos.write(buffer, 0, len);
        }
        bos.close();
        return bos.toByteArray();
    }

    public static String getContentType(String fileName) {
        String contentType = "application/octet-stream";
        String[] fileNameList = fileName.split("\\.");
        if(fileNameList.length==1){
            return contentType;
        }
        String fileExt = fileNameList[fileNameList.length-1];
        if (fileExt != null) {
            fileExt = fileExt.toLowerCase();
            switch (fileExt) {
                case "txt":
                    contentType = "text/plain";
                    break;
                case "html":
                case "htm":
                    contentType = "text/html";
                    break;
                case "css":
                    contentType = "text/css";
                    break;
                case "js":
                    contentType = "application/javascript";
                    break;
                case "json":
                    contentType = "application/json";
                    break;
                case "xml":
                    contentType = "application/xml";
                    break;
                case "jpg":
                case "jpeg":
                    contentType = "image/jpeg";
                    break;
                case "gif":
                    contentType = "image/gif";
                    break;
                case "png":
                    contentType = "image/png";
                    break;
                case "svg":
                    contentType = "image/svg+xml";
                    break;
                case "mp3":
                    contentType = "audio/mpeg";
                    break;
                case "mp4":
                    contentType = "video/mp4";
                    break;
                case "avi":
                    contentType = "video/avi";
                    break;
                case "mov":
                    contentType = "video/quicktime";
                    break;
                case "doc":
                case "docx":
                    contentType = "application/msword";
                    break;
                case "xls":
                case "xlsx":
                    contentType = "application/vnd.ms-excel";
                    break;
                case "ppt":
                case "pptx":
                    contentType = "application/vnd.ms-powerpoint";
                    break;
                case "rar":
                    contentType = "application/x-rar-compressed";
                    break;
                case "zip":
                    contentType = "application/zip";
                    break;
                case "tar":
                    contentType = "application/x-tar";
                    break;
                case "gz":
                    contentType = "application/x-gzip";
                    break;
            }
        }
        return contentType;
    }

    public static void writeFile(String fileName, String content) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(fileName);
            fos.write(content.getBytes("UTF-8"));
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (Exception e) {

            }
        }
    }

    public static String readFileContent(String fileName) {
        String fileContent = "";
        FileInputStream fis = null;
        InputStreamReader isr = null;
        try {
            fis = new FileInputStream(fileName);
            isr = new InputStreamReader(fis, "UTF-8");
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                fileContent += line;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (isr != null) {
                    isr.close();
                }
                if (fis != null) {
                    fis.close();
                }
            } catch (Exception e) {

            }
        }
        return fileContent;
    }

    public static void copyFile(String sourceFilePath, String tagretFilePath) throws IOException {
        try {
            File file = new File(tagretFilePath);
            if (file.exists()) {
                file.delete();
            }
            InputStream in = new FileInputStream(sourceFilePath);
            OutputStream out = new FileOutputStream(tagretFilePath);
            byte[] buffer = new byte[2048];
            int nBytes = 0;
            while ((nBytes = in.read(buffer)) > 0) {
                out.write(buffer, 0, nBytes);
            }
            out.flush();
            out.close();
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    

    // The invalid character list is derived from this Stackoverflow page.
    // https://stackoverflow.com/questions/1155107/is-there-a-cross-platform-java-method-to-remove-filename-special-chars
    private final static int[] INVALID_CHARS = {34, 60, 62, 124, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 58, 42, 63, 92, 47, 32};

    static {
        Arrays.sort(INVALID_CHARS);
    }
    
    
    public static String getExtension(String fileName) {
    	int pos = fileName.lastIndexOf('.');
        if (pos<0)
            return "";
        String ext = fileName.substring(pos);
        return ext.toLowerCase();
    }
    
    public static String getFilename(final String filename) {
        if (filename == null) {
            return null;
        }        
        int index = filename.lastIndexOf('/');
        if(index>=0) {
        	return filename.substring(index + 1);
        }
        index = filename.lastIndexOf('\\');
        if(index>=0) {
        	return filename.substring(index + 1);
        }
        return filename;
    }
    
    /**
     * Replaces invalid characters for a file system name within a given filename string to underscore '_'.
     * Be careful not to pass a file path as this method replaces path delimiter characters (i.e forward/back slashes).
     * @param filename The filename to clean
     * @return sanitized filename
     */
    public static String sanitizeFilename(String filename) {
        if (filename == null || filename.isEmpty()) {
            return filename;
        }
        int codePointCount = filename.codePointCount(0, filename.length());

        final StringBuilder cleanName = new StringBuilder();
        for (int i = 0; i < codePointCount; i++) {
            int c = filename.codePointAt(i);
            if (Arrays.binarySearch(INVALID_CHARS, c) < 0) {
                cleanName.appendCodePoint(c);
            } else {
                cleanName.append('_');
            }
        }
        return cleanName.toString();
    }
    
    /**
     * Replaces invalid characters for a file system name within a given filename string to underscore '_'.
     * Be careful not to pass a file path as this method replaces path delimiter characters (i.e forward/back slashes).
     * @param filename The filename to clean
     * @return sanitized filename
     */
    public static String sanitizePathname(String filename) {
        if (filename == null || filename.isEmpty()) {
            return filename;
        }
        int codePointCount = filename.codePointCount(0, filename.length());

        final StringBuilder cleanName = new StringBuilder();
        for (int i = 0; i < codePointCount; i++) {
            int c = filename.codePointAt(i);
            if(c=='/' || c=='\\') {
            	cleanName.appendCodePoint('/');
            }
            else if (Arrays.binarySearch(INVALID_CHARS, c) < 0) {
                cleanName.appendCodePoint(c);
            } else {
                cleanName.append('_');
            }
        }
        return cleanName.toString();
    }
}
