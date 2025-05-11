package org.apache.ignite.internal.processors.rest.igfs.controller;


import org.apache.ignite.internal.processors.rest.igfs.config.RangeConverter;
import org.apache.ignite.internal.processors.rest.igfs.config.SystemConfig;
import org.apache.ignite.internal.processors.rest.igfs.model.*;
import org.apache.ignite.internal.processors.rest.igfs.service.S3Service;
import org.apache.ignite.internal.processors.rest.igfs.util.*;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.annotation.Blocking;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/*
 * api参考地址 https://docs.aws.amazon.com/AmazonS3/latest/API/
 * @author admin
 *
 */
@RestController
@RequestMapping("/s3")
@CrossOrigin
@Blocking
public class S3VertxletHandler {
	
	private static final String RANGES_BYTES = "bytes";
	
	private static final String RANGE = "Range";

	private static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

	private static final String HEADER_X_AMZ_META_PREFIX = "x-amz-meta-";
	
	private static final String HEADER_X_AMZ_COPY_SOURCE = "x-amz-copy-source";
	
	private static final String contextPath = "/s3/";
	
    @Autowired
    private S3Service s3Service;
    
    @Autowired
    @Qualifier("systemConfig")
    private SystemConfig systemConfig;    

    
    private Map<String, String> getUserMetadata(final HttpServerRequest request) {
        return request.headers().entries().stream()
            .filter(header -> header.getKey().startsWith(HEADER_X_AMZ_META_PREFIX))
            .collect(Collectors.toMap(
                header -> header.getKey().substring(HEADER_X_AMZ_META_PREFIX.length()),
                header -> header.getValue()
            ));
     }

    // Bucket相关接口
    @PutMapping("/:bucketName")
    public ResponseEntity<String> createBucket(@PathVariable String bucketName,HttpServerRequest request) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        if (s3Service.headBucket(bucketName)) {
            return ResponseEntity.status(409).body("BucketAlreadyExists");
        }
        s3Service.createBucket(bucketName);
        String endpoint = systemConfig.getEndpointOverride();
        if(endpoint==null || endpoint.isBlank()) {
    		endpoint = CommonUtil.getApiPath(request) ;
    	}
        return ResponseEntity.ok().location(new URI(endpoint+"/"+bucketName+"/")).build();
    }

    @GetMapping("/")
    public ResponseEntity<String> listBuckets() throws Exception {
        String xml = "";
        ListBucketsResult result = new ListBucketsResult(s3Service.listBuckets());
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("ListAllMyBucketsResult");
        Element owner = root.addElement("Owner");
        Element ownerId = owner.addElement("ID");       
        Element displayName = owner.addElement("DisplayName");
        
        Element buckets = root.addElement("Buckets");
        for (Bucket item : result.getBuckets()) {
            Element bucket = buckets.addElement("Bucket");
            Element name = bucket.addElement("Name");
            name.setText(item.getName());
            ownerId.setText(item.getAuthor());
            displayName.setText(item.getAuthor());
            Element creationDate = bucket.addElement("CreationDate");
            creationDate.setText(item.getCreationDate());
        }
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        StringWriter out = new StringWriter();
        XMLWriter writer = new XMLWriter(out, format);
        writer.write(doc);
        writer.close();
        xml = out.toString();
        out.close();
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }

    @RequestMapping(value = "/:bucketName", method = RequestMethod.HEAD)
    public ResponseEntity<Object> headBucket(@PathVariable(value = "bucketName") String bucketName,HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        if (s3Service.headBucket(bucketName)) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/:bucketName")
    public ResponseEntity<String> deleteBucket(@PathVariable String bucketName) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        s3Service.deleteBucket(bucketName);
        return ResponseEntity.noContent().build();
    }

    // List Object相关接口
    @GetMapping("/:bucketName")
    public void listObjects(@PathVariable String bucketName, HttpServerRequest request,HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");        
        String prefix = ConvertOp.convert2String(request.getParam("prefix"));
        boolean delimiter = request.getParam("delimiter")!=null;
        _listObjects(bucketName,prefix,delimiter,response);
    }
    
    private void _listObjects(@PathVariable String bucketName, String prefix,boolean delimiter,HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");        
        
        List<S3Object> s3ObjectList = s3Service.listObjects(bucketName, prefix);
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("ListBucketResult");
        Element name = root.addElement("Name");
        name.setText(bucketName);
        Element prefixElement = root.addElement("Prefix");
        prefixElement.setText(prefix);
        if(delimiter) {
        	Element delimiterElement = root.addElement("Delimiter");
        	delimiterElement.setText("/");     	
        }
        Element isTruncated = root.addElement("IsTruncated");
        isTruncated.setText("false");
        Element maxKeys = root.addElement("MaxKeys");
        maxKeys.setText("5000");
        Element countKeys = root.addElement("KeyCount");
        countKeys.setText(""+s3ObjectList.size());
        Set<String> prefixs = new TreeSet<>();
        for (S3Object s3Object : s3ObjectList) {
        	if(delimiter) {
        		if(prefix==null || prefix.isEmpty()) {
        			if(s3Object.getKey().endsWith("/")) {
        				prefixs.add(s3Object.getKey());
        				continue;
        			}
        		}
        		else if(s3Object.getKey().endsWith("/")) {        			
        			prefixs.add(s3Object.getKey());
        			continue;
        		}
        		else if(s3Object.getKey().startsWith(prefix)) {
        			String[] filePathList = s3Object.getKey().split("/");
        	        StringBuilder result = new StringBuilder();
        	        for (int i = 0; i < filePathList.length - 1; i++) {
        	            result.append(filePathList[i]).append("/");
        	        }
        			prefixs.add(result.toString());   			
        		}
        		
        	}
            Element contents = root.addElement("Contents");
            Element key = contents.addElement("Key");
            key.setText(s3Object.getKey());
            if (!StringUtils.isEmpty(s3Object.getMetadata().getLastModified())) {
                Element lastModified = contents.addElement("LastModified");
                lastModified.setText(DateUtil.getDateIso8601Format(s3Object.getMetadata().getLastModified()));                
            }
            Element size = contents.addElement("Size");
            size.setText(s3Object.getMetadata().getContentLength() + "");
            
            Element ETag = contents.addElement("ETag");
            ETag.setText(s3Object.getMetadata().getETag());
            
            Element StorageClass = contents.addElement("StorageClass");
            StorageClass.setText("STANDARD");
            
        }
        if(delimiter) {
        	Element prefixesElement = root.addElement("CommonPrefixes");
        	for(String p : prefixs) {
	        	Element Prefix = prefixesElement.addElement("Prefix");
	        	Prefix.setText(p);
        	}
        }
        response.putHeader("ContentType", MediaType.APPLICATION_XML.toString()+";charset=UTF-8");        
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");     
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XMLWriter writer = new XMLWriter(out, format);
        writer.write(doc);
        writer.flush();
        
        response.end(Buffer.buffer(out.toByteArray()));
    }


    @RequestMapping(value = "/:bucketName/*", method = RequestMethod.HEAD)
    public void headObject(@PathVariable String bucketName, HttpServerRequest request, HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.uri(), "utf-8");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        ObjectMetadata metadata = s3Service.headObject(bucketName, objectKey);
        if (metadata==null) {
        	response.setStatusCode(404);
        	response.end();
        } else {
        	HashMap<String, String> headInfo = new HashMap<>();
        	
            if(metadata.getContentLength()>0 && !metadata.getFileName().endsWith("/")) {
            	headInfo.put("Content-Disposition", "filename=" + URLEncoder.encode(metadata.getFileName(), "utf-8"));
            	headInfo.put("Content-Length", ""+metadata.getContentLength());
            }
            headInfo.put("Content-Type", metadata.getContentType());
            headInfo.put("Last-Modified", DateUtil.getDateGMTFormat(metadata.getLastModified()));                
            headInfo.put("ETag","\"" + metadata.getETag() + "\"");
            
            for (String key : headInfo.keySet()) {
                response.putHeader(key, headInfo.get(key));
            }

        }
    }
    
   

    @PutMapping("/:bucketName/*")
    public ResponseEntity<String> putObject(@PathVariable String bucketName, RoutingContext rc, HttpServerRequest request,HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.uri(), "utf-8");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        String copySource = request.getHeader(HEADER_X_AMZ_COPY_SOURCE);
        if(copySource!=null) {
        	copySource = URLDecoder.decode(copySource, "utf-8");
        }        
        
        if (!StringUtils.hasText(copySource)) {
        	Map<String,String> userMeta = getUserMetadata(request);
        	userMeta.put("ownerName", CommonUtil.getCurrentUser(rc));
        	byte[] bytes = rc.body().buffer().getBytes();
            InputStream inputStream = new ByteArrayInputStream(bytes);
        	s3Service.putObject(bucketName, objectKey, inputStream, userMeta);
        	ObjectMetadata metadata = s3Service.headObject(bucketName, objectKey);
            response.putHeader("Content-Disposition", "filename=" + URLEncoder.encode(metadata.getFileName(), "utf-8"));            
            response.putHeader("Last-Modified", DateUtil.getDateGMTFormat(metadata.getLastModified()));                
            response.putHeader("ETag","\"" + metadata.getETag() + "\"");            
            return ResponseEntity.ok().build();
        } else {
            if (copySource.indexOf("\\?") >= 0) {
                copySource = copySource.split("\\?")[0];
            }
            if(copySource.startsWith("/")) {
            	copySource = copySource.substring(1);
            }
            String[] copyList = copySource.split("\\/");
            String sourceBucketName = "";
            for (String item : copyList) {
                if (!StringUtils.isEmpty(item)) {
                    sourceBucketName = item;
                    break;
                }
            }

            StringBuilder result = new StringBuilder();
            for (int i = 1; i < copyList.length; i++) {
                result.append(copyList[i]).append("/");
            }
            String sourceObjectKey = result.toString();
            s3Service.copyObject(sourceBucketName, sourceObjectKey, bucketName, objectKey);
            ObjectMetadata metadata = s3Service.headObject(bucketName, objectKey);
            String xml = "";
            Document doc = DocumentHelper.createDocument();
            Element root = doc.addElement("CopyObjectResult");
            Element lastModified = root.addElement("LastModified");
            lastModified.setText(DateUtil.getDateIso8601Format(metadata.getLastModified()));
            Element eTag = root.addElement("ETag");
            eTag.setText(metadata.getETag());
            OutputFormat format = OutputFormat.createPrettyPrint();
            format.setEncoding("utf-8");
            StringWriter out = new StringWriter();
            XMLWriter writer = new XMLWriter(out, format);
            writer.write(doc);
            writer.close();
            xml = out.toString();
            out.close();
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
        }
    }

    @GetMapping("/:bucketName/*")
    public void getOrListObject(@PathVariable String bucketName, 
    		@RequestHeader(value = RANGE, required = false) String rangeStr,
    		HttpServerRequest request, HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.uri(), "utf-8");        
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        ObjectMetadata metadata = s3Service.headObject(bucketName, objectKey);
        if (metadata == null ) {
        	response.setStatusCode(404);
        	response.end();
        } 
        else if(metadata.getContentLength()==0 && metadata.getFileName().endsWith("/")) {
        	boolean delimiter = request.getParam("delimiter")!=null;
        	_listObjects(bucketName,objectKey,delimiter,response);
        }
        else {        	
        	Range range = null;
        	long bytesToRead = metadata.getContentLength();
        	if(rangeStr!=null && !rangeStr.isEmpty()) {
        		RangeConverter conv = new RangeConverter();
        		range = conv.convert(rangeStr);
        		
        		final long fileSize = metadata.getContentLength();
                bytesToRead = Math.min(fileSize - 1, range.getEnd()) - range.getStart() + 1;

                if (bytesToRead < 0 || fileSize < range.getStart()) {
                  response.setStatusCode(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE.value());
                  response.end();
                  return;
                }
                
                response.setStatusCode(HttpStatus.PARTIAL_CONTENT.value());
                response.putHeader(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES);
                
        	}
        	
        	S3ObjectInputStream objectStream = s3Service.getObject(bucketName, objectKey, range);        	
        	
        	response.putHeader("Content-Disposition", "filename=" + URLEncoder.encode(metadata.getFileName(), "utf-8"));            
            response.putHeader("Last-Modified", DateUtil.getDateGMTFormat(metadata.getLastModified()));                
            response.putHeader("ETag","\""+metadata.getETag()+"\"");
            response.putHeader("Content-Type",metadata.getContentType() + ";charset="+metadata.getContentEncoding());
            response.putHeader("Content-Length",String.valueOf(bytesToRead));
            if(range!=null) {            	
            	bytesToRead = objectStream.getMetadata().getContentLength();
                response.putHeader("Content-Length",String.valueOf(bytesToRead));
            	response.putHeader(HttpHeaders.CONTENT_RANGE,
                        String.format("bytes %s-%s", range.getStart(), bytesToRead + range.getStart() - 1));              
            }
            
            int bufsize = Math.min(1024*16,(int)(1024+bytesToRead/1024*1024));
            byte[] buff = new byte[bufsize];
            Buffer buffer = Buffer.buffer(bufsize);
            int i = 0;
            
            try {
                while ((i = objectStream.read(buff)) != -1) {
                	buffer.setBytes(0, buff, 0, i);
                	response.write(buffer);
                }
                response.end();
            } catch (EOFException e) {                
                // ignore     
            } catch (IOException e) {                
            	e.printStackTrace();
            	response.setStatusCode(404);
            	response.end();
            } finally {
                try {
                	objectStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }        
        
    }

    @DeleteMapping("/:bucketName/*")
    public ResponseEntity<String> deleteObject(@PathVariable String bucketName, HttpServerRequest request) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.uri(), "utf-8");
        if (pageUrl.indexOf("\\?") >= 0) {
            pageUrl = pageUrl.split("\\?")[0];
        }
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        
        if(request.getParam("uploadId")!=null) { 
        	// AbortMultipartUpload
        	String uploadId = request.getParam("uploadId");
        	s3Service.abortMultipartUpload(bucketName, objectKey, uploadId);
        }
        else {
        	s3Service.deleteObject(bucketName, objectKey);
        }
        
        return ResponseEntity.noContent().build();
    }
    
    // 分片上传
    @PostMapping(value = "/:bucketName/*")
    public ResponseEntity<String> multipartUpload(@PathVariable String bucketName, RoutingContext rc, HttpServerRequest request) throws Exception {
    	if(request.getParam("uploads")!=null) {
    		return createMultipartUpload(bucketName,rc,request);
    	}
    	if(request.getParam("uploadId")!=null) {
    		return completeMultipartUpload(bucketName,rc,request);
    	}
    	return ResponseEntity.badRequest().build();
    }   


    // 分片上传初始化
    public ResponseEntity<String> createMultipartUpload(@PathVariable String bucketName, RoutingContext rc, HttpServerRequest request) throws Exception {
    	Map<String,String> userMeta = getUserMetadata(request);
    	userMeta.put("ownerName", CommonUtil.getCurrentUser(rc));
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.uri(), "utf-8");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        InitiateMultipartUploadResult result = s3Service.initiateMultipartUpload(bucketName, objectKey, userMeta);

        String xml = "";
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("InitiateMultipartUploadResult");
        Element bucket = root.addElement("Bucket");
        bucket.setText(bucketName);
        Element key = root.addElement("Key");
        key.setText(objectKey);
        Element uploadId = root.addElement("UploadId");
        uploadId.setText(result.getUploadId());
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        StringWriter out = new StringWriter();
        XMLWriter writer = new XMLWriter(out, format);
        writer.write(doc);
        writer.close();
        xml = out.toString();
        out.close();
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }

    @PutMapping(value = "/:bucketName/*", params = {"partNumber", "uploadId"})
    public ResponseEntity<String> uploadPart(@PathVariable String bucketName, RoutingContext rc, HttpServerRequest request, HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.uri(), "utf-8");
        
        String contentMD5 = request.getHeader("Content-MD5");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        int partNumber = ConvertOp.convert2Int(request.getParam("partNumber"));
        String uploadId = request.getParam("uploadId");
        byte[] bytes = rc.body().buffer().getBytes();
        InputStream inputStream = new ByteArrayInputStream(bytes);
        PartETag eTag = s3Service.uploadPart(bucketName, objectKey, partNumber, uploadId, inputStream);
        response.putHeader("ETag", "\""+eTag.geteTag()+"\"");
        return ResponseEntity.ok().build();
    }

   
    public ResponseEntity<String> completeMultipartUpload(@PathVariable String bucketName, RoutingContext rc, HttpServerRequest request) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.uri(), "utf-8");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        String uploadId = request.getParam("uploadId");
        List<PartETag> partETags = new ArrayList<>();

        SAXReader reader = new SAXReader();
        byte[] bytes = rc.body().buffer().getBytes();
        InputStream inputStream = new ByteArrayInputStream(bytes);
        Document bodyDoc = reader.read(inputStream);
        Element bodyRoot = bodyDoc.getRootElement();
        List<Element> elementList = bodyRoot.elements("Part");
        for (Element element : elementList) {
            int partNumber = ConvertOp.convert2Int(element.element("PartNumber").getText());
            String eTag = element.element("ETag").getText();
            PartETag partETag = new PartETag(partNumber, eTag);
            partETags.add(partETag);
        }        
        String ownerName = CommonUtil.getCurrentUser(rc);
        CompleteMultipartUploadResult result = s3Service.completeMultipartUpload(bucketName, objectKey, uploadId, ownerName, new CompleteMultipartUpload(partETags));
        String xml = "";
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("CompleteMultipartUploadResult");
        Element location = root.addElement("Location");
        location.setText(CommonUtil.getApiPath(request) + "" + bucketName + "/" + objectKey);
        Element bucket = root.addElement("Bucket");
        bucket.setText(bucketName);
        Element key = root.addElement("Key");
        key.setText(objectKey);
        Element etag = root.addElement("ETag");
        etag.setText(result.geteTag());

        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        StringWriter out = new StringWriter();
        XMLWriter writer = new XMLWriter(out, format);
        writer.write(doc);
        writer.close();
        xml = out.toString();
        out.close();
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }
}
