/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tools.release;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Create HTML release report based on Jira Issues
 */
public class ReleaseReportGenerator {
    /** Issue fields list for return from Jira search */
    private final static String[] jiraFields = new String[] {"key", "summary"};

    /** Release report json tempate path */
    private static String templatePath = "./report_template.json";

    /** Release report css file path */
    private static String cssPath = "./report_template.css";

    /**
     * Generate reports.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If generating report execution failed.
     */
    public static void main(String[] args) throws Exception {
        parseArguments(args);

        JSONParser parser = new JSONParser();

        JSONObject template = (JSONObject)parser.parse(readFile(templatePath));

        String css = readFile(cssPath);

        PrintWriter writer = new PrintWriter((String)template.get("outfile"), "UTF-8");

        writer.println(generateHTMLReleaseReport(template, css));

        writer.close();
    }

    /**
     * @param Path path to file
     * @return String content.
     * @throws IOException If failed.
     */
    private static String readFile(String Path) throws IOException {
        StringBuilder sb = new StringBuilder();

        BufferedReader br = new BufferedReader(new FileReader(Path));
        String line;

        while ((line = br.readLine()) != null)
            sb.append(line);

        return sb.toString();
    }

    /**
     * Parse startup arguments
     *
     * @param args Arguments read from command line
     */
    private static void parseArguments(String... args) {
        for (String arg : args) {
            if (arg.toLowerCase().startsWith("--templatepath="))
                templatePath = arg.substring(15);
            else if (arg.toLowerCase().startsWith("-tm="))
                templatePath = arg.substring(4);
            else if (arg.toLowerCase().startsWith("--csspath="))
                cssPath = arg.substring(10);
            else if (arg.toLowerCase().startsWith("-css="))
                cssPath = arg.substring(5);
        }
    }

    /**
     * Generate HTML release report
     *
     * @throws HttpException On search failed throws exception
     */
    private static String generateHTMLReleaseReport(JSONObject template,
        String templateCss) throws HttpException, ParseException {
        StringBuilder htmlReport = new StringBuilder("<head>\n<style>" + templateCss + "</style>\n</head>\n");

        htmlReport.append("<body>\n");

        htmlReport.append("<h1>").append((String)template.get("header")).append("</h1>\n");

        htmlReport.append("<div>").append((String)template.get("description")).append("</div>");

        for (Object item : (JSONArray)template.get("items"))
            htmlReport.append(buildReportForTemplateItem(template, (JSONObject)item));

        htmlReport.append("</body>");

        return htmlReport.toString();
    }

    /**
     * Build HTML report part for template item
     *
     * @param template template for get parent settings
     * @param item item for build report part
     * @return String with report if issues founded for conditions in jql's
     * @throws HttpException If Jira search throw exception
     */
    private static String buildReportForTemplateItem(JSONObject template,
        JSONObject item) throws HttpException, ParseException {
        StringBuilder itemReport = new StringBuilder("<h2>" + item.get("header") + "</h2>\n");

        itemReport.append("<ul>\n");

        for (Object search : (JSONArray)item.get("search")) {
            JSONObject srv = getJsonObjectFromArrayById((long)((JSONObject)search).get("server"),
                "id", (JSONArray)template.get("servers"));

            if (srv != null)
                itemReport.append(buildReportForSearch((JSONObject)search, srv));
        }

        itemReport.append("</ul>");

        return itemReport.toString().contains("<li>") ? itemReport.toString() : "";
    }

    /**
     * Get JsonObject from JsonArray by id
     *
     * @param id id of object
     * @param fieldName name of id json field
     * @param arr JsonArray for search
     * @return JsonObject of id exist in arr of null if none
     */
    private static JSONObject getJsonObjectFromArrayById(long id, String fieldName, JSONArray arr) {
        for (Object item : arr) {
            if ((long)((JSONObject)item).get(fieldName) == id)
                return (JSONObject)item;
        }

        return null;
    }

    /**
     * Build report part for search with personal settings
     *
     * @param search JsonObject with search settings
     * @return HTML formatted string
     */
    private static String buildReportForSearch(JSONObject search, JSONObject srv) throws HttpException, ParseException {
        StringBuilder sr = new StringBuilder();

        String baseUrl = (String)srv.get("baseurl");

        String kField = (String)search.get("key");

        if (kField == null)
            kField = "key";

        String kPtrn = (String)search.get("kptrn");

        String sField = (String)search.get("summary");

        if (sField == null)
            sField = "fields.summary";

        String sPtrn = (String)search.get("sptrn");

        String[] jf = srv.get("fields").toString()
                .replace("[", "").replace("]", "").split(",");

        if (jf == null)
            jf = jiraFields;

        List<JSONObject> issues = searchIssues((String)srv.get("apiurl"),
            (String)srv.get("username"),
            (String)srv.get("password"),
            (String)search.get("jql"),
            jf);

        for (JSONObject issue : issues) {
            String key = getFieldFromJsonByPattern(issue, kField, kPtrn);
            String summary = getFieldFromJsonByPattern(issue, sField, sPtrn);

            if (key == null || summary == null)
                continue;

            if (summary.lastIndexOf(".") == summary.length() - 1)
                summary = summary.substring(0, summary.length() - 1);

            sr.append("<li>");
            sr.append(summary);

            if ((boolean)search.get("showlink")) {
                sr.append("<a href=\"");
                sr.append(baseUrl);
                sr.append(key);
                sr.append("\"> [#");
                sr.append(key);
                sr.append("]</a>\n");
            }
            else {
                sr.append(" <span>[#");
                sr.append(key);
                sr.append("]</span>");
            }

            sr.append("</li>\n");
        }
        return sr.toString();
    }

    /**
     * Search issues in jira using jql filter
     *
     * @param jql jql for search issues
     * @param fields issue fields name for return in response
     * @return List if issues implemented as JSONObject
     * @throws HttpException All error throws HttpException
     */
    private static List<JSONObject> searchIssues(final String jiraApiUrl, final String jiraUsername,
        final String jiraApiPwd, final String jql, final String[] fields) throws HttpException, ParseException {
        final List<JSONObject> issues = new CopyOnWriteArrayList<>();
        final int startAt = 0;
        final int maxResults = 50;

        JSONObject firstRes = executeSearchRequest(jiraApiUrl, jiraUsername, jiraApiPwd, jql, fields,
            startAt, maxResults);

        final long total = (long)firstRes.get("total");

        if (firstRes.get("issues") != null) {
            for (Object issue : (JSONArray)firstRes.get("issues"))
                issues.add((JSONObject)issue);
        }

        ExecutorService svc = Executors.newFixedThreadPool(16);

        Collection<Future<Void>> futs = new ArrayList<>();

        for (double i = 0; i < (double)total / 50; i++) {
            final int ost = (int)i + 1;

            futs.add(svc.submit(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    JSONObject jiraRes = executeSearchRequest(jiraApiUrl, jiraUsername, jiraApiPwd, jql,
                        fields, maxResults * ost, maxResults);

                    if ((long)jiraRes.get("total") != total)
                        throw new HttpException("Total count of issues changed, please restart report");

                    if (jiraRes.get("issues") != null) {
                        for (Object issue : (JSONArray)jiraRes.get("issues"))
                            issues.add((JSONObject)issue);
                    }

                    return null;
                }
            }));
        }

        try {
            for (Future<Void> fut : futs)
                fut.get();
        }
        catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException ex) {
            throw new HttpException(ex.toString());
        }
        finally {
            svc.shutdown();
        }

        return issues;
    }

    /**
     * Get search page with offset
     *
     * @param jql jql for search issues
     * @param fields issue fields name for return in response
     * @param startAt startAt issue with index in search
     * @param maxResults maxResult per page
     * @return JSONObject with jira response
     * @throws HttpException On search failed
     */
    private static JSONObject executeSearchRequest(String jiraApiUrl, String jiraUsername, String jiraPwd, String jql,
        String[] fields, int startAt, int maxResults) throws HttpException, ParseException {
        JSONObject data = new JSONObject();

        data.put("jql", jql);
        data.put("fields", Arrays.asList(fields));
        data.put("startAt", startAt);
        data.put("maxResults", maxResults);

        JSONParser parser = new JSONParser();

        if (!jiraApiUrl.endsWith("/"))
            jiraApiUrl += "/";

        HttpRequestBase req = buildRequestWithData("POST", jiraApiUrl + "search", jiraUsername, jiraPwd,
            data.toString(), ContentType.APPLICATION_JSON);

        return (JSONObject)parser.parse(executeRequest(req));
    }

    /**
     * Execute HttpRequest and return result as String
     *
     * @param req HttpBaseRequest for execution
     * @return String read from HttpResponse
     * @throws HttpException If status code not in (200, 201, 204) or IOException caught on execute or read response
     */
    private static String executeRequest(HttpRequestBase req) throws HttpException {
        int statusCode;
        HttpResponse httpRes;
        HttpClient client = HttpClientBuilder.create().build();
        StringBuilder res = new StringBuilder();

        try {
            httpRes = client.execute(req);

            statusCode = httpRes.getStatusLine().getStatusCode();

            if (statusCode != 204) {
                BufferedReader br = new BufferedReader(
                    new InputStreamReader((httpRes.getEntity().getContent())));

                String line;

                while ((line = br.readLine()) != null)
                    res.append(line);
            }
        }
        catch (IOException e) {
            throw new HttpException(String.format("IOException caught on read request response. Message: %s", e.getMessage()));
        }

        if (statusCode == 200 || statusCode == 201 || statusCode == 204)
            return res.toString();
        else
            throw new HttpException(String.format("Status code: %d! Response: %s", statusCode, res));
    }

    /**
     * Build new HttpRequest without data
     *
     * @param type Request type GET, PUT, POST or DELETE, GET by default
     * @param url Full url with url argumenets if needed
     * @param username Username for basic authentication if needed
     * @param pwd Password for basic authentication if needed
     * @return HttpRequestBase for execute
     * @throws HttpException On bad request type
     */
    private static HttpRequestBase buildRequest(String type, String url, String username,
        String pwd) throws HttpException {
        HttpRequestBase httpReq = new HttpGet(url);

        switch (type.toUpperCase()) {
            case "GET":
                break;
            case "PUT":
                httpReq = new HttpPut(url);
                break;
            case "POST":
                httpReq = new HttpPost(url);
                break;
            case "DELETE":
                httpReq = new HttpDelete(url);
                break;
            default:
                throw new HttpException("Type of request should be 'GET', 'PUT', 'POST' or 'DELETE'");
        }

        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(pwd))
            setAuthenticationHeader(httpReq, username, pwd);

        return httpReq;
    }

    /**
     * Build new HttpRequest with data and content type
     *
     * @param type Request type GET, PUT, POST or DELETE, GET by default
     * @param url Full url with url arguments if needed
     * @param username Username for basic authentication if needed
     * @param pwd Password for basic authentication if needed
     * @param data Json or another data string
     * @param contentType Content type of data in string
     * @return HttpRequestBase for execute
     * @throws HttpException On bad request type
     */
    private static HttpRequestBase buildRequestWithData(String type, String url, String username, String pwd,
        String data, ContentType contentType) throws HttpException {
        HttpRequestBase httpReq = buildRequest(type, url, username, pwd);

        if (type.toUpperCase().equals("GET") || type.toUpperCase().equals("DELETE"))
            throw new HttpException("GET and DELETE requests can't contains data");
        else
            setRequestEntity((HttpEntityEnclosingRequestBase)httpReq, data, contentType);

        return httpReq;
    }

    /**
     * Set Basic Authentication Header by build base64 string and add it into Authorization Header
     *
     * @param httpReq for add header
     * @param username Username for basic authentication
     * @param pwd Password for authentication
     */
    private static void setAuthenticationHeader(HttpRequestBase httpReq, String username, String pwd) {
        httpReq.setHeader("Authorization",
            "Basic " + new String(Base64.encodeBase64((username + ":" + pwd).getBytes(Charset.forName("ISO-8859-1")))));
    }

    /**
     * Set data with type into httpReq
     *
     * @param httpReq HttpEntityEnclosingRequestBase for add data
     * @param data JSON or another data string
     * @param contentType Content type of data in string
     */
    private static void setRequestEntity(HttpEntityEnclosingRequestBase httpReq, String data, ContentType contentType) {
        StringEntity input = new StringEntity(data, contentType);

        httpReq.setEntity(input);
    }

    /**
     * Get field value from jsonObject as string by field name and regex pattern
     *
     * @param jsonObject json object for read field value
     * @param fieldName field name for read value. Name support . as sub-object separator.
     * @param regex regex for search substring in value
     * @return Value or substring of value if regex is not null
     */
    private static String getFieldFromJsonByPattern(JSONObject jsonObject, String fieldName, String regex) {
        String[] fArray = fieldName.split("\\.");
        String retval = null;

        for (int i = 0; i < fArray.length; i++) {
            if (i != fArray.length - 1)
                jsonObject = (JSONObject)jsonObject.get(fArray[i]);
            else
                retval = (String)jsonObject.get(fArray[i]);
        }

        if (regex == null)
            return retval;

        if (retval == null)
            return retval;

        Pattern ptrn = Pattern.compile(regex);
        Matcher matcher = ptrn.matcher(retval);

        if (matcher.find())
            return matcher.group(1);

        return null;
    }
}