package org.apache.ignite.tools.release;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.text.SimpleDateFormat;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Create HTML release report based on Jira Issues
 */
public class ReleaseReportGenerator {
    /** Jira search DateTime format */
    private final static SimpleDateFormat jiraSearchDTF = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    /** Issue fields list for return from Jira search */
    private final static String[] jiraFields = new String[] {"key", "summary", "description"};

    /** Release report json tempate path */
    private static String templatePath = "./report_template.json";

    /** Release report css file path */
    private static String cssPath="./report_template.css";

    /**
     * Generate reports.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If generating report execution failed.
     */
    public static void main(String... args) throws Exception {
        parseArguments(args);

        String sCurrentLine;
        String templateJsonStr = "";
        BufferedReader br = new BufferedReader(new FileReader(templatePath));
        while ((sCurrentLine = br.readLine()) != null)
            templateJsonStr += sCurrentLine;

        JSONObject template = new JSONObject(templateJsonStr);

        String templateCss = "";
        br = new BufferedReader(new FileReader(templatePath));
        while ((sCurrentLine = br.readLine()) != null)
            templateCss += sCurrentLine;

        PrintWriter writer = new PrintWriter(template.getString("outfile"), "UTF-8");
        writer.println(generateHTMLReleaseReport(template, templateCss));
        writer.close();
    }

    /**
     * Parse startup arguments
     *
     * @param args Arguments read from command line
     */
    private static void parseArguments(String... args) {
        for (String arg : args) {
            if (arg.toLowerCase().startsWith("--templatePath=") || arg.toLowerCase().startsWith("-tm="))
                templatePath = arg.toLowerCase().replace("--templatePath=", "").replace("-tm=", "");
            else if ((arg.toLowerCase().startsWith("--cssPath=") || arg.toLowerCase().startsWith("-css=")))
                cssPath = arg.toLowerCase().replace("--cssPath=", "").replace("-css=", "");
        }
    }

    /**
     * Generate HTML release report
     *
     * @throws HttpException On search failed throws exception
     */
    private static String generateHTMLReleaseReport(JSONObject template, String templateCss) throws HttpException {
        String htmlReport = "<head>\n<style>" + templateCss + "</style>\n</head>\n";

        htmlReport += "<body>\n";

        htmlReport += "<h1>" + template.getString("header") + "</h1>\n";

        htmlReport += "<div>" + template.getString("description") + "</div>";

        for (Object item : template.getJSONArray("items")) {
            htmlReport += buildReportForTemplateItem(template, (JSONObject) item);
        }

        htmlReport += "</body>";

        return htmlReport;
    }

    /**
     * Build HTML report part for template item
     *
     * @param template template for get parent settings
     * @param item item for build report part
     * @return String with report if issues founded for conditions in jql's
     * @throws HttpException If Jira search throw exception
     */
    private static String buildReportForTemplateItem(JSONObject template, JSONObject item) throws HttpException{
        String itemReport = "<h2>" + item.getString("header") + "</h2>\n";

        itemReport += "<ul>\n";

        for (Object search : item.getJSONArray("search")) {
            JSONObject srv = getJsonObjectFromArrayById(((JSONObject) search).getInt("server"), "id", template.getJSONArray("servers"));
            if (srv != null)
                itemReport += buildReportForSearch((JSONObject) search, srv);
        }
        itemReport += "</ul>";

        return itemReport.contains("<li>") ? itemReport : "";
    }

    /**
     * Get JsonObject from JsonArray by id
     *
     * @param id id of object
     * @param fieldName name of id json field
     * @param arr JsonArray for search
     * @return JsonObject of id exist in arr of null if none
     */
    private static JSONObject getJsonObjectFromArrayById(int id, String fieldName, JSONArray arr) {
        for (Object item : arr) {
            if (((JSONObject) item).getInt(fieldName) == id)
                return (JSONObject) item;
        }
        return null;
    }

    /**
     *  Build report part for search with personal settings
     *
     * @param search JsonObject with search settings
     * @return HTML formatted string
     */
    private static String buildReportForSearch(JSONObject search, JSONObject srv) throws HttpException{
        String searchReport = "";

        List<JSONObject> issues = searchIssues(srv.getString("apiurl"), srv.getString("username"), srv.getString("password"), search.getString("jql"), jiraFields);

        for (JSONObject issue : issues) {
            String summary = issue.getJSONObject("fields").getString("summary");

            if (summary.lastIndexOf(".") == summary.length() - 1)
                summary = summary.substring(0, summary.length() - 1);

                searchReport += "<li>" + summary;

                if (search.getBoolean("showlink"))
                    searchReport += "<a href=\"https://issues.apache.org/jira/browse/" + issue.getString("key") + "\"> [#" + issue.getString("key") + "]</a>\n";
                else
                    searchReport +=  " <span>[#" + issue.getString("key") + "]</span>";

            searchReport +=  "</li>\n";
        }
        return searchReport;
    }

    /**
     * Search issues in jira using jql filter
     *
     * @param jql jql for search issues
     * @param fields issue fields name for return in response
     * @return List if issues implemented as JSONObject
     * @throws HttpException All error throws HttpException
     */
    private static List<JSONObject> searchIssues(final String jiraApiUrl, final String jiraUsername, final String jiraApiPwd, final String jql, final String[] fields) throws HttpException {
        final List<JSONObject> issues = new CopyOnWriteArrayList<> ();
        final int startAt = 0;
        final int maxResults = 50;

        JSONObject firstRes = executeSearchRequest(jiraApiUrl, jiraUsername, jiraApiPwd, jql, fields, startAt, maxResults);

        final int total = firstRes.getInt("total");

        if (!firstRes.isNull("issues")) {
            for (Object issue : firstRes.getJSONArray("issues")) {
                issues.add((JSONObject) issue);
            }
        }

        ExecutorService svc = Executors.newFixedThreadPool(16);
        Collection<Future<Void>> futs = new ArrayList<>();
        for (double i = 0; i < (double) total / 50; i++) {
            final int ost = (int) i + 1;

            futs.add(svc.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    JSONObject jiraRes = executeSearchRequest(jiraApiUrl, jiraUsername, jiraApiPwd, jql, fields, maxResults * ost, maxResults);

                    if (jiraRes.getInt("total") != total)
                        throw new HttpException("Total count of issues changed, please restart report");

                    if (!jiraRes.isNull("issues")) {
                        for (Object issue : jiraRes.getJSONArray("issues")) {
                            issues.add((JSONObject) issue);
                        }
                    }

                    return null;
                }
            }));
        }

        try {
            for (Future<Void> fut : futs)
                fut.get();
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException ex) {
            throw new HttpException(ex.toString());
        }

        svc.shutdown();

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
    private static JSONObject executeSearchRequest(String jiraApiUrl, String jiraUsername, String jiraPwd, String jql, String[] fields, int startAt, int maxResults) throws HttpException {
        JSONObject data = new JSONObject();
        data.put("jql", jql);
        data.put("fields", fields);
        data.put("startAt", startAt);
        data.put("maxResults", maxResults);

        if (!jiraApiUrl.endsWith("/"))
            jiraApiUrl+= "/";

        HttpRequestBase req = buildRequestWithData("POST", jiraApiUrl + "search", jiraUsername, jiraPwd, data.toString(), ContentType.APPLICATION_JSON);

        return new JSONObject(executeRequest(req));
    }

    /**
     * Execute HttpRequest and return result as String
     *
     * @param req HttpBaseRequest for execution
     * @return String read from HttpResponse
     * @throws HttpException If status code not in (200, 201, 204) or IOException caught on execute or read response
     */
    private static String executeRequest(HttpRequestBase req) throws HttpException{
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
        } catch (IOException e) {
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
    private static HttpRequestBase buildRequest(String type, String url, String username, String pwd) throws HttpException {
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
    private static HttpRequestBase buildRequestWithData(String type, String url, String username, String pwd, String data, ContentType contentType) throws HttpException {
        HttpRequestBase httpReq = buildRequest(type, url, username, pwd);

        if (type.toUpperCase().equals("GET") || type.toUpperCase().equals("DELETE"))
            throw new HttpException("GET and DELETE requests can't contains data");
        else
            setRequestEntity((HttpEntityEnclosingRequestBase) httpReq, data, contentType);

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
}