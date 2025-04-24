package org.apache.ignite.internal.processors.rest.igfs.intecept;

import org.apache.ignite.internal.processors.rest.igfs.config.SystemConfig;
import org.apache.ignite.internal.processors.rest.igfs.util.ConvertOp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;


import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.impl.UserImpl;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.Vertxlet;
import io.vertx.webmvc.annotation.VertxletMapping;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Component
@VertxletMapping(url="/s3/*")
public class S3Intecept extends Vertxlet{
    
	@Autowired
    private SystemConfig systemConfig;
	
	@Override
    public void handle(RoutingContext rc) {
		try {
			preHandle(rc.request(),rc.response(),rc);
			rc.next();
		} catch (Exception e) {			
			e.printStackTrace();
			rc.fail(e);
		}
	}

    
    public boolean preHandle(HttpServerRequest request, HttpServerResponse response, RoutingContext rc) throws Exception {
        boolean flag = false;
        
        String authorization = request.getHeader("Authorization");
        if(StringUtils.hasText(authorization)){
            flag = validAuthorizationHead(rc,request, systemConfig.getAccessKey(), systemConfig.getSecretAccessKey());
        }else{
            authorization = request.getParam("X-Amz-Credential");
            if(StringUtils.hasText(authorization)){
                flag = validAuthorizationUrl(rc,request, systemConfig.getAccessKey(), systemConfig.getSecretAccessKey());
            }
            else {
            	flag = true; // 匿名访问            	
            }
        }
        if(!flag){
            response.setStatusCode(HttpStatus.UNAUTHORIZED.value());
        }
        return flag;
    }

    public boolean validAuthorizationHead(RoutingContext rc,HttpServerRequest request, String accessKeyId, String secretAccessKey) throws Exception {
        String authorization = request.getHeader("Authorization");
        String requestDate = request.getHeader("x-amz-date");
        String contentHash = request.getHeader("x-amz-content-sha256");
        String httpMethod = request.method().name();
        String uri = request.uri().split("\\?")[0];
        String queryString = ConvertOp.convert2String(request.query());
        //示例
        //AWS4-HMAC-SHA256 Credential=admin/20230530/us-east-1/s3/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;host;x-amz-content-sha256;x-amz-date, Signature=6f50628a101b46264c7783937be0366762683e0d319830b1844643e40b3b0ed

        ///region authorization拆分
        String[] parts = authorization.trim().split("\\,");
        //第一部分-凭证范围
        String credential = parts[0].split("\\=")[1];
        String[] credentials = credential.split("\\/");
        String accessKey = credentials[0];
        if (!accessKeyId.equals(accessKey)) {
            return false;
        }
        String date = credentials[1];
        String region = credentials[2];
        String service = credentials[3];
        String aws4Request = credentials[4];
        //第二部分-签名头中包含哪些字段
        String signedHeader = parts[1].split("\\=")[1];
        String[] signedHeaders = signedHeader.split("\\;");
        //第三部分-生成的签名
        String signature = parts[2].split("\\=")[1];
        ///endregion

        ///region 待签名字符串
        String stringToSign = "";
        //签名由4部分组成
        //1-Algorithm – 用于创建规范请求的哈希的算法。对于 SHA-256，算法是 AWS4-HMAC-SHA256。
        stringToSign += "AWS4-HMAC-SHA256" + "\n";
        //2-RequestDateTime – 在凭证范围内使用的日期和时间。
        stringToSign += requestDate + "\n";
        //3-CredentialScope – 凭证范围。这会将生成的签名限制在指定的区域和服务范围内。该字符串采用以下格式：YYYYMMDD/region/service/aws4_request
        stringToSign += date + "/" + region + "/" + service + "/" + aws4Request + "\n";
        //4-HashedCanonicalRequest – 规范请求的哈希。
        //<HTTPMethod>\n
        //<CanonicalURI>\n
        //<CanonicalQueryString>\n
        //<CanonicalHeaders>\n
        //<SignedHeaders>\n
        //<HashedPayload>
        String hashedCanonicalRequest = "";
        //4.1-HTTP Method
        hashedCanonicalRequest += httpMethod + "\n";
        //4.2-Canonical URI
        hashedCanonicalRequest += uri + "\n";
        //4.3-Canonical Query String
        if(StringUtils.hasText(queryString)){
            Map<String, String> queryStringMap =  parseQueryParams(queryString);
            List<String> keyList = new ArrayList<>(queryStringMap.keySet());
            Collections.sort(keyList);
            StringBuilder queryStringBuilder = new StringBuilder("");
            for (String key:keyList) {
                queryStringBuilder.append(key).append("=").append(queryStringMap.get(key)).append("&");
            }
            queryStringBuilder.deleteCharAt(queryStringBuilder.lastIndexOf("&"));

            hashedCanonicalRequest += queryStringBuilder.toString() + "\n";
        }else{
            hashedCanonicalRequest += queryString + "\n";
        }
        //4.4-Canonical Headers
        for (String name : signedHeaders) {
            hashedCanonicalRequest += name + ":" + request.getHeader(name) + "\n";
        }
        hashedCanonicalRequest += "\n";
        //4.5-Signed Headers
        hashedCanonicalRequest += signedHeader + "\n";
        //4.6-Hashed Payload
        hashedCanonicalRequest += contentHash;
        stringToSign += doHex(hashedCanonicalRequest);
        ///endregion

        ///region 重新生成签名
        //计算签名的key
        byte[] kSecret = ("AWS4" + secretAccessKey).getBytes("UTF8");
        byte[] kDate = doHmacSHA256(kSecret, date);
        byte[] kRegion = doHmacSHA256(kDate, region);
        byte[] kService = doHmacSHA256(kRegion, service);
        byte[] signatureKey = doHmacSHA256(kService, aws4Request);
        //计算签名
        byte[] authSignature = doHmacSHA256(signatureKey, stringToSign);
        //对签名编码处理
        String strHexSignature = doBytesToHex(authSignature);
        ///endregion

        if (signature.equals(strHexSignature)) {
        	rc.put("accessKey", accessKey);
        	JsonObject principal = JsonObject.of("username",accessKey);
        	JsonObject attrs = new JsonObject();
        	rc.setUser(new UserImpl(principal,attrs));
            return true;
        }
        return false;
    }

    public boolean validAuthorizationUrl(RoutingContext rc,HttpServerRequest request, String accessKeyId, String secretAccessKey) throws Exception {
        String requestDate = request.getParam("X-Amz-Date");
        String contentHash = "UNSIGNED-PAYLOAD";
        String httpMethod = request.method().name();
        String uri = request.uri().split("\\?")[0];
        String queryString = ConvertOp.convert2String(request.query());
        //示例
        //"http://localhost:8001/s3/kkk/%E6%B1%9F%E5%AE%81%E8%B4%A2%E6%94%BF%E5%B1%80%E9%A1%B9%E7%9B%AE%E5%AF%B9%E6%8E%A5%E6%96%87%E6%A1%A3.docx?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230531T024715Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=admin%2F20230531%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=038e2ea71073761aa0370215621599649e9228177c332a0a79f784b1a6d9ee39

        ///region 参数准备
        //第一部分-凭证范围
        String credential =request.getParam("X-Amz-Credential");
        String[] credentials = credential.split("\\/");
        String accessKey = credentials[0];
        if (!accessKeyId.equals(accessKey)) {
            return false;
        }
        String date = credentials[1];
        String region = credentials[2];
        String service = credentials[3];
        String aws4Request = credentials[4];
        //第二部分-签名头中包含哪些字段
        String signedHeader = request.getParam("X-Amz-SignedHeaders");
        String[] signedHeaders = signedHeader.split("\\;");
        //第三部分-生成的签名
        String signature = request.getParam("X-Amz-Signature");
        ///endregion

        ///region 验证expire
        String expires = request.getParam("X-Amz-Expires");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
        LocalDateTime startDate = LocalDateTime.parse(requestDate,formatter);
        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime localDateTime = startDate.atZone(ZoneId.of("UTC")).withZoneSameInstant(zoneId);
        startDate = localDateTime.toLocalDateTime();
        LocalDateTime endDate = startDate.plusSeconds(ConvertOp.convert2Int(expires));
        if(endDate.isBefore(LocalDateTime.now())){
            return false;
        }
        ///endregion

        ///region 待签名字符串
        String stringToSign = "";
        //签名由4部分组成
        //1-Algorithm – 用于创建规范请求的哈希的算法。对于 SHA-256，算法是 AWS4-HMAC-SHA256。
        stringToSign += "AWS4-HMAC-SHA256" + "\n";
        //2-RequestDateTime – 在凭证范围内使用的日期和时间。
        stringToSign += requestDate + "\n";
        //3-CredentialScope – 凭证范围。这会将生成的签名限制在指定的区域和服务范围内。该字符串采用以下格式：YYYYMMDD/region/service/aws4_request
        stringToSign += date + "/" + region + "/" + service + "/" + aws4Request + "\n";
        //4-HashedCanonicalRequest – 规范请求的哈希。
        //<HTTPMethod>\n
        //<CanonicalURI>\n
        //<CanonicalQueryString>\n
        //<CanonicalHeaders>\n
        //<SignedHeaders>\n
        //<HashedPayload>
        String hashedCanonicalRequest = "";
        //4.1-HTTP Method
        hashedCanonicalRequest += httpMethod + "\n";
        //4.2-Canonical URI
        hashedCanonicalRequest += uri + "\n";
        //4.3-Canonical Query String
        if(StringUtils.hasText(queryString)){
            Map<String, String> queryStringMap =  parseQueryParams(queryString);
            List<String> keyList = new ArrayList<>(queryStringMap.keySet());
            Collections.sort(keyList);
            StringBuilder queryStringBuilder = new StringBuilder("");
            for (String key:keyList) {
                if(!key.equals("X-Amz-Signature")){
                    queryStringBuilder.append(key).append("=").append(queryStringMap.get(key)).append("&");
                }
            }
            queryStringBuilder.deleteCharAt(queryStringBuilder.lastIndexOf("&"));

            hashedCanonicalRequest += queryStringBuilder.toString() + "\n";
        }else{
            hashedCanonicalRequest += queryString + "\n";
        }
        //4.4-Canonical Headers
        for (String name : signedHeaders) {
            hashedCanonicalRequest += name + ":" + request.getHeader(name) + "\n";
        }
        hashedCanonicalRequest += "\n";
        //4.5-Signed Headers
        hashedCanonicalRequest += signedHeader + "\n";
        //4.6-Hashed Payload
        hashedCanonicalRequest += contentHash;
        stringToSign += doHex(hashedCanonicalRequest);
        ///endregion

        ///region 重新生成签名
        //计算签名的key
        byte[] kSecret = ("AWS4" + secretAccessKey).getBytes("UTF8");
        byte[] kDate = doHmacSHA256(kSecret, date);
        byte[] kRegion = doHmacSHA256(kDate, region);
        byte[] kService = doHmacSHA256(kRegion, service);
        byte[] signatureKey = doHmacSHA256(kService, aws4Request);
        //计算签名
        byte[] authSignature = doHmacSHA256(signatureKey, stringToSign);
        //对签名编码处理
        String strHexSignature = doBytesToHex(authSignature);
        ///endregion

        if (signature.equals(strHexSignature)) {
        	rc.put("accessKey", accessKey);
        	JsonObject principal = JsonObject.of("username",accessKey);
        	JsonObject attrs = new JsonObject();
        	rc.setUser(new UserImpl(principal,attrs));
            return true;
        }
        return false;
    }

    private String doHex(String data) {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(data.getBytes("UTF-8"));
            byte[] digest = messageDigest.digest();
            return String.format("%064x", new java.math.BigInteger(1, digest));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private byte[] doHmacSHA256(byte[] key, String data) throws Exception {
        String algorithm = "HmacSHA256";
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(data.getBytes("UTF8"));
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    private String doBytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars).toLowerCase();
    }

    public static Map<String, String> parseQueryParams(String queryString) {
        Map<String, String> queryParams = new HashMap<>();
        try {
            if (queryString != null && !queryString.isEmpty()) {
                String[] queryParamsArray = queryString.split("\\&");

                for (String param : queryParamsArray) {
                    String[] keyValue = param.split("\\=");
                    if (keyValue.length == 1) {
                        String key = keyValue[0];
                        String value = "";
                        queryParams.put(key, value);
                    }
                    else if (keyValue.length == 2) {
                        String key = keyValue[0];
                        String value = keyValue[1];
                        queryParams.put(key, value);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queryParams;
    }

}
