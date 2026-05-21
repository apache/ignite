package org.apache.ignite.internal.processors.rest.igfs.controller;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.VertxInstanceAware;
import io.vertx.webmvc.annotation.Blocking;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processors.rest.igfs.config.RangeConverter;
import org.apache.ignite.internal.processors.rest.igfs.config.SystemConfig;
import org.apache.ignite.internal.processors.rest.igfs.service.Impl.S3IgfsServiceImpl;
import org.apache.ignite.internal.processors.rest.igfs.service.Impl.S3LocalFileServiceImpl;
import org.apache.ignite.internal.processors.rest.igfs.service.S3Service;
import org.apache.ignite.internal.processors.rest.igfs.util.CommonUtil;
import org.apache.ignite.internal.rest.igfs.model.*;
import org.apache.ignite.internal.rest.igfs.util.ConvertOp;
import org.apache.ignite.internal.rest.igfs.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;
import static org.apache.ignite.internal.processors.rest.igfs.util.CommonUtil.removeQuery;
/*
 * api参考地址 https://docs.aws.amazon.com/AmazonS3/latest/API/
 * @author admin
 *
 */
@RestController
@RequestMapping("/s3")
@CrossOrigin
@Blocking
public class S3VertxRestController extends VertxInstanceAware{
	
	private static final String RANGES_BYTES = "bytes";
	
	private static final String RANGE = "Range";

	private static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

	private static final String HEADER_X_AMZ_META_PREFIX = "x-amz-meta-";
	
	private static final String HEADER_X_AMZ_COPY_SOURCE = "x-amz-copy-source";
	
	private static final String contextPath = "/s3/";

    // 创建 DocumentBuilderFactory 和 DocumentBuilder
    private DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
    private TransformerFactory transformerFactory = TransformerFactory.newInstance();
	
	private Map<String,S3Service> s3ServiceMap = new HashMap<>();   
    
    @Autowired
    @Qualifier("systemConfig")
    private SystemConfig systemConfig;

    /**
     * 将 DOM Document 转换为格式化的 XML 字符串
     */
    private String domToString(Document doc) throws Exception {
        // 方式一：使用 Transformer（支持格式化）

        Transformer transformer = transformerFactory.newTransformer();

        // 设置输出属性
        transformer.setOutputProperty(OutputKeys.ENCODING, "utf-8");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");


        StringWriter writer = new StringWriter();
        StreamResult result = new StreamResult(writer);
        DOMSource source = new DOMSource(doc);

        transformer.transform(source, result);

        return writer.toString();
    }

    
    private Map<String, String> getUserMetadata(final HttpServerRequest request) {
        return request.headers().entries().stream()
            .filter(header -> header.getKey().startsWith(HEADER_X_AMZ_META_PREFIX))
            .collect(Collectors.toMap(
                header -> header.getKey().substring(HEADER_X_AMZ_META_PREFIX.length()),
                header -> header.getValue()
            ));
     }
    
    private S3Service s3Service() {
    	String region = this.getIgniteInstanceName();
    	S3Service s3 = s3ServiceMap.get(region);
    	if(s3==null) {
    		try {
    			Ignite ignite = Ignition.ignite(region);
                if(!ignite.fileSystems().isEmpty())
    			    s3 = new S3IgfsServiceImpl(region,systemConfig);
                else
                    s3 = new S3LocalFileServiceImpl(region,systemConfig);
    		}
    		catch(Exception e) {
    			s3 = new S3LocalFileServiceImpl(region,systemConfig);
    		}
    		s3ServiceMap.put(region, s3);
    	}
    	return s3;
    }

    // Bucket相关接口
    @PutMapping("/:bucketName")
    public ResponseEntity<String> createBucket(@PathVariable String bucketName,HttpServerRequest request) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        if (s3Service().headBucket(bucketName)) {
            return ResponseEntity.status(409).body("BucketAlreadyExists");
        }
        s3Service().createBucket(bucketName);
        String endpoint = systemConfig.getEndpointOverride();
        if(endpoint==null || endpoint.isBlank()) {
    		endpoint = CommonUtil.getApiPath(request) ;
    	}
        return ResponseEntity.ok().location(new URI(endpoint+"/"+bucketName+"/")).build();
    }

    @GetMapping("/")
    public ResponseEntity<String> listBuckets() throws Exception {
        String xml = "";
        ListBucketsResult result = new ListBucketsResult(s3Service().listBuckets());

        // 创建 DocumentBuilder
        DocumentBuilder builder = documentFactory.newDocumentBuilder();

        // 创建 DOM 文档
        Document doc = builder.newDocument();

        // 创建根元素
        Element root = doc.createElement("ListAllMyBucketsResult");
        doc.appendChild(root);

        // 创建 Owner 元素
        Element owner = doc.createElement("Owner");
        root.appendChild(owner);

        Element ownerId = doc.createElement("ID");
        owner.appendChild(ownerId);

        Element displayName = doc.createElement("DisplayName");
        owner.appendChild(displayName);

        // 创建 Buckets 元素
        Element buckets = doc.createElement("Buckets");
        root.appendChild(buckets);

        // 遍历 buckets
        for (Bucket item : result.getBuckets()) {
            Element bucket = doc.createElement("Bucket");
            buckets.appendChild(bucket);

            Element name = doc.createElement("Name");
            name.setTextContent(item.getName());
            bucket.appendChild(name);

            ownerId.setTextContent(item.getAuthor());
            displayName.setTextContent(item.getAuthor());

            Element creationDate = doc.createElement("CreationDate");
            creationDate.setTextContent(item.getCreationDate());
            bucket.appendChild(creationDate);

            Element region = doc.createElement("Region");
            region.setTextContent(item.getRegion());
            bucket.appendChild(region);

            String endpoint = systemConfig.getEndpointOverride();
            if (endpoint != null) {
                Element location = doc.createElement("Location");
                location.setTextContent(endpoint + "/" + item.getName() + "/");
                bucket.appendChild(location);
            }
        }

        // 将 DOM 转换为 XML 字符串（带格式化）
        xml = domToString(doc);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }

    @RequestMapping(value = "/:bucketName", method = RequestMethod.HEAD)
    public ResponseEntity<Object> headBucket(@PathVariable(value = "bucketName") String bucketName,HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        if (s3Service().headBucket(bucketName)) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/:bucketName")
    public ResponseEntity<String> deleteBucket(@PathVariable String bucketName) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        s3Service().deleteBucket(bucketName);
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
        
        List<S3Object> s3ObjectList = s3Service().listObjects(bucketName, prefix);

        DocumentBuilder builder = documentFactory.newDocumentBuilder();

        // 创建 DOM 文档
        Document doc = builder.newDocument();

        // 创建根元素
        Element root = doc.createElement("ListBucketResult");
        doc.appendChild(root);

        // Name 元素
        Element name = doc.createElement("Name");
        name.setTextContent(bucketName);
        root.appendChild(name);

        // Prefix 元素
        Element prefixElement = doc.createElement("Prefix");
        prefixElement.setTextContent(prefix != null ? prefix : "");
        root.appendChild(prefixElement);

        // Delimiter 元素（可选）
        if (delimiter) {
            Element delimiterElement = doc.createElement("Delimiter");
            delimiterElement.setTextContent("/");
            root.appendChild(delimiterElement);
        }

        // IsTruncated 元素
        Element isTruncated = doc.createElement("IsTruncated");
        isTruncated.setTextContent("false");
        root.appendChild(isTruncated);

        // MaxKeys 元素
        Element maxKeys = doc.createElement("MaxKeys");
        maxKeys.setTextContent("10000");
        root.appendChild(maxKeys);

        // KeyCount 元素
        Element countKeys = doc.createElement("KeyCount");
        countKeys.setTextContent(String.valueOf(s3ObjectList.size()));
        root.appendChild(countKeys);

        Set<String> prefixs = new TreeSet<>();

        for (S3Object s3Object : s3ObjectList) {
            if (delimiter) {
                if (prefix == null || prefix.isEmpty()) {
                    if (s3Object.getKey().endsWith("/")) {
                        prefixs.add(s3Object.getKey());
                        continue;
                    }
                } else if (s3Object.getKey().endsWith("/")) {
                    prefixs.add(s3Object.getKey());
                    continue;
                } else if (s3Object.getKey().startsWith(prefix)) {
                    String[] filePathList = s3Object.getKey().split("/");
                    StringBuilder result = new StringBuilder();
                    for (int i = 0; i < filePathList.length - 1; i++) {
                        result.append(filePathList[i]).append("/");
                    }
                    prefixs.add(result.toString());
                }
            }

            // Contents 元素
            Element contents = doc.createElement("Contents");
            root.appendChild(contents);

            Element key = doc.createElement("Key");
            key.setTextContent(s3Object.getKey());
            contents.appendChild(key);

            if (!StringUtils.isEmpty(s3Object.getMetadata().getLastModified())) {
                Element lastModified = doc.createElement("LastModified");
                lastModified.setTextContent(DateUtil.getDateIso8601Format(s3Object.getMetadata().getLastModified()));
                contents.appendChild(lastModified);
            }

            Element size = doc.createElement("Size");
            size.setTextContent(s3Object.getMetadata().getContentLength() + "");
            contents.appendChild(size);

            Element ETag = doc.createElement("ETag");
            ETag.setTextContent(s3Object.getMetadata().getETag());
            contents.appendChild(ETag);

            Element StorageClass = doc.createElement("StorageClass");
            StorageClass.setTextContent("STANDARD");
            contents.appendChild(StorageClass);
        }

        if (delimiter && !prefixs.isEmpty()) {
            Element prefixesElement = doc.createElement("CommonPrefixes");
            root.appendChild(prefixesElement);
            for (String p : prefixs) {
                Element Prefix = doc.createElement("Prefix");
                Prefix.setTextContent(p);
                prefixesElement.appendChild(Prefix);
            }
        }

        // 将 DOM 转换为字节数组并响应
        String xmlBytes = domToString(doc);

        response.putHeader("ContentType", MediaType.APPLICATION_XML.toString() + ";charset=UTF-8");
        response.end(Buffer.buffer(xmlBytes));
    }


    @RequestMapping(value = "/:bucketName/*", method = RequestMethod.HEAD)
    public void headObject(@PathVariable String bucketName, HttpServerRequest request, HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(removeQuery(request.uri()), "utf-8");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        ObjectMetadata metadata = s3Service().headObject(bucketName, objectKey);
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
            response.end();
        }
    }
    
   

    @PutMapping("/:bucketName/*")
    public ResponseEntity<String> putObject(@PathVariable String bucketName, RoutingContext rc, HttpServerRequest request,HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(removeQuery(request.uri()), "utf-8");
        
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
        	s3Service().putObject(bucketName, objectKey, inputStream, userMeta);
        	ObjectMetadata metadata = s3Service().headObject(bucketName, objectKey);
            response.putHeader("Content-Disposition", "filename=" + URLEncoder.encode(metadata.getFileName(), "utf-8"));            
            response.putHeader("Last-Modified", DateUtil.getDateGMTFormat(metadata.getLastModified()));                
            response.putHeader("ETag","\"" + metadata.getETag() + "\"");            
            return ResponseEntity.ok().build();

        } else {
            copySource = removeQuery(copySource);
            if(copySource.startsWith("/")) {
            	copySource = copySource.substring(1);
            }
            String[] copyList = copySource.split("\\/",2);
            String sourceBucketName = copyList[0];
            String sourceObjectKey = copyList[1];
            s3Service().copyObject(sourceBucketName, sourceObjectKey, bucketName, objectKey);
            ObjectMetadata metadata = s3Service().headObject(bucketName, objectKey);

            DocumentBuilder builder = documentFactory.newDocumentBuilder();
            // 创建 DOM 文档
            Document doc = builder.newDocument();
            Element root = doc.createElement("CopyObjectResult");
            doc.appendChild(root);

            Element lastModified = doc.createElement("LastModified");
            lastModified.setTextContent(DateUtil.getDateIso8601Format(metadata.getLastModified()));

            Element eTag = doc.createElement("ETag");
            eTag.setTextContent(metadata.getETag());

            root.appendChild(lastModified);
            root.appendChild(eTag);

            String xml = domToString(doc);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
        }
    }

    @GetMapping("/:bucketName/*")
    public void getOrListObject(@PathVariable String bucketName, 
    		@RequestHeader(value = RANGE, required = false) String rangeStr,
    		HttpServerRequest request, HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(removeQuery(request.uri()), "utf-8");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        ObjectMetadata metadata = s3Service().headObject(bucketName, objectKey);
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
        	
        	S3ObjectInputStream objectStream = s3Service().getObject(bucketName, objectKey, range);        	
        	
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
        String pageUrl = URLDecoder.decode(removeQuery(request.uri()), "utf-8");
        if (pageUrl.indexOf("\\?") >= 0) {
            pageUrl = pageUrl.split("\\?")[0];
        }
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        
        if(request.getParam("uploadId")!=null) { 
        	// AbortMultipartUpload
        	String uploadId = request.getParam("uploadId");
        	s3Service().abortMultipartUpload(bucketName, objectKey, uploadId);
        }
        else {
        	s3Service().deleteObject(bucketName, objectKey);
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
        String pageUrl = URLDecoder.decode(removeQuery(request.uri()), "utf-8");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        InitiateMultipartUploadResult result = s3Service().initiateMultipartUpload(bucketName, objectKey, userMeta);

        String xml = "";

        DocumentBuilder builder = documentFactory.newDocumentBuilder();

        // 创建 DOM 文档
        Document doc = builder.newDocument();

        // 创建根元素
        Element root = doc.createElement("InitiateMultipartUploadResult");
        doc.appendChild(root);

        // Bucket 元素
        Element bucket = doc.createElement("Bucket");
        bucket.setTextContent(bucketName);
        root.appendChild(bucket);

        // Key 元素
        Element key = doc.createElement("Key");
        key.setTextContent(objectKey);
        root.appendChild(key);

        // UploadId 元素
        Element uploadId = doc.createElement("UploadId");
        uploadId.setTextContent(result.getUploadId());
        root.appendChild(uploadId);

        xml = this.domToString(doc);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }

    @PutMapping(value = "/:bucketName/*", params = {"partNumber", "uploadId"})
    public ResponseEntity<String> uploadPart(@PathVariable String bucketName, RoutingContext rc, HttpServerRequest request, HttpServerResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(removeQuery(request.uri()), "utf-8");
        
        String contentMD5 = request.getHeader("Content-MD5");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        int partNumber = ConvertOp.convert2Int(request.getParam("partNumber"));
        String uploadId = request.getParam("uploadId");
        byte[] bytes = rc.body().buffer().getBytes();
        InputStream inputStream = new ByteArrayInputStream(bytes);
        PartETag eTag = s3Service().uploadPart(bucketName, objectKey, partNumber, uploadId, inputStream);
        response.putHeader("ETag", "\""+eTag.geteTag()+"\"");
        return ResponseEntity.ok().build();
    }

   
    public ResponseEntity<String> completeMultipartUpload(@PathVariable String bucketName, RoutingContext rc, HttpServerRequest request) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(removeQuery(request.uri()), "utf-8");
        
        String objectKey = pageUrl.replace(contextPath + bucketName + "/", "");
        String uploadId = request.getParam("uploadId");
        List<PartETag> partETags = new ArrayList<>();

        byte[] bytes = rc.body().buffer().getBytes();
        InputStream inputStream = new ByteArrayInputStream(bytes);


        DocumentBuilder builder = documentFactory.newDocumentBuilder();
        Document bodyDoc = builder.parse(inputStream);

        Element bodyRoot = bodyDoc.getDocumentElement();
        NodeList partNodes = bodyRoot.getElementsByTagName("Part");

        for (int i = 0; i < partNodes.getLength(); i++) {
            Element partElement = (Element) partNodes.item(i);

            NodeList partNumberNodes = partElement.getElementsByTagName("PartNumber");
            int partNumber = ConvertOp.convert2Int(partNumberNodes.item(0).getTextContent());

            NodeList etagNodes = partElement.getElementsByTagName("ETag");
            String eTag = etagNodes.item(0).getTextContent();

            partETags.add(new PartETag(partNumber, eTag));
        }

        String ownerName = CommonUtil.getCurrentUser(rc);
        CompleteMultipartUploadResult result = s3Service().completeMultipartUpload(bucketName, objectKey, uploadId, ownerName, new CompleteMultipartUpload(partETags));

        // 构建响应 XML
        Document responseDoc = builder.newDocument();
        Element root = responseDoc.createElement("CompleteMultipartUploadResult");
        responseDoc.appendChild(root);

        Element location = responseDoc.createElement("Location");
        location.setTextContent(CommonUtil.getApiPath(request) + "" + bucketName + "/" + objectKey);
        root.appendChild(location);

        Element bucket = responseDoc.createElement("Bucket");
        bucket.setTextContent(bucketName);
        root.appendChild(bucket);

        Element key = responseDoc.createElement("Key");
        key.setTextContent(objectKey);
        root.appendChild(key);

        Element etag = responseDoc.createElement("ETag");
        etag.setTextContent(result.geteTag());
        root.appendChild(etag);

        String xml = this.domToString(responseDoc);

        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }
}