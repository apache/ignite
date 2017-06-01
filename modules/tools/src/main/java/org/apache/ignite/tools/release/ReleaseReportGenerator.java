package org.apache.ignite.tools.release;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Calendar;
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
import org.json.JSONObject;


/**
 * Create HTML release report based on Jira Issues
 */
public class ReleaseReportGenerator {
    /** Jira search DateTime format */
    private final static SimpleDateFormat _jiraSearchDTF = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    /** Issue fields list for return from Jira search */
    private final static String[] jiraFields = new String[] {"key", "summary", "description"};

    /** Jira API Url. Using Apache Ignite Jira by default */
    private static String jiraApiUrl = "https://issues.apache.org/jira/rest/api/2/";

    /** Jira API username. Authentication don't needed be default */
    private static String jiraUsername = "";

    /** Jira API password. Authentication don't needed be default */
    private static String jiraPwd = "";

    /** Projects list for search issues included in release (comma separeted ) */
    private static String jiraProjects = "IGNITE";

    /** Issue fix version for search issues included in release */
    private static String jiraFixVer = "";

    /** Release report header. */
    private static String reportHdr = "Apache IGNITE";

    /**
     * Generate reports.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String... args) throws Exception {
        parseArguments(args);

        if (jiraFixVer.equals(""))
            throw new Exception("Product version should be set by command line arguments");

        PrintWriter writer = new PrintWriter("./apache-ignite-"+ jiraFixVer +".html", "UTF-8");
        writer.println(generateHTMLReleaseReport());
        writer.close();
        generateHTMLReleaseReport();
    }

    /**
     * Parse startup arguments
     *
     * @param args Arguments read from command line
     */
    private static void parseArguments(String... args) {
        for (String arg : args) {
            if (arg.toLowerCase().startsWith("--jiraurl="))
                jiraApiUrl = arg.substring(10);
            else if (arg.toLowerCase().startsWith("--jirausername="))
                jiraUsername = arg.substring(15);
            else if (arg.toLowerCase().startsWith("--jirapassword="))
                jiraPwd = arg.substring(15);
            else if (arg.toLowerCase().startsWith("--jirafixversion="))
                jiraFixVer = arg.substring(17);
            else if (arg.toLowerCase().startsWith("--jiraprojects="))
                jiraProjects = arg.substring(15);
            else if (arg.toLowerCase().startsWith("--reportHdr="))
                reportHdr = arg.substring(12);
        }

        if (!jiraApiUrl.endsWith("/"))
            jiraApiUrl+= "/";
    }

    /**
     * Generate HTML release report
     *
     * @throws HttpException On search failed throws exception
     */
    private static String generateHTMLReleaseReport() throws HttpException {
        String htmlReport = "<head>" + buildReleaseReportCss() + "</head>\n";

        htmlReport += "<body>\n";

        htmlReport += "<h1 id=\"" + reportHdr + " " + jiraFixVer + "\">"+ reportHdr + " " + jiraFixVer + "</h1>\n";

        //New features not .NET
        htmlReport += buildReportForIssueList("New Features",searchIssues(
            String.format("project in (%s) and fixVersion = %s and type in (\"New Feature\") and component != documentation " +
            "and ((labels != .net and labels != .NET) or labels is EMPTY) and status in (Closed, Resolved)", jiraProjects, jiraFixVer) +
            " & updated <= '" + _jiraSearchDTF.format(Calendar.getInstance().getTime()) + "'", jiraFields));

        //New features .NET
        htmlReport += buildReportForIssueList("New Features .NET",searchIssues(
            String.format("project in (%s) and fixVersion = %s and type in (\"New Feature\") and component != documentation " +
            "and (labels = .net or labels = .NET)  and status in (Closed, Resolved)", jiraProjects, jiraFixVer) +
            " & updated <= '" + _jiraSearchDTF.format(Calendar.getInstance().getTime()) + "'", jiraFields));
        //Improvements not .NET
        htmlReport += buildReportForIssueList("Improvements",searchIssues(
            String.format("project in (%s) and fixVersion = %s and type in (Improvement) and component != documentation " +
            "and ((labels != .net and labels != .NET) or labels is EMPTY) and status in (Closed, Resolved)", jiraProjects, jiraFixVer) +
            " & updated <= '" + _jiraSearchDTF.format(Calendar.getInstance().getTime()) + "'", jiraFields));

        //Improvements .NET
        htmlReport += buildReportForIssueList("Improvements .NET",searchIssues(
            String.format("project in (%s) and fixVersion = %s and type in (Improvement) and component != documentation " +
            "and (labels = .net or labels = .NET)  and status in (Closed, Resolved)", jiraProjects, jiraFixVer) +
            " & updated <= '" + _jiraSearchDTF.format(Calendar.getInstance().getTime()) + "'", jiraFields));

        //Bugs not .NET
        htmlReport += buildReportForIssueList("Fixed",searchIssues(
            String.format("project in (%s) and fixVersion = %s and affectedVersion != %s and type in (Bug) and component != documentation " +
            "and ((labels != .net and labels != .NET) or labels is EMPTY) and status in (Closed, Resolved)", jiraProjects, jiraFixVer, jiraFixVer) +
            " & updated <= '" + _jiraSearchDTF.format(Calendar.getInstance().getTime()) + "'", jiraFields));

        //Bugs .NET
        htmlReport += buildReportForIssueList("Fixed .NET",searchIssues(
            String.format("project in (%s) and fixVersion = %s and affectedVersion != %s and type in (Bug) and component != documentation " +
            "and (labels = .net or labels = .NET) and status in (Closed, Resolved)", jiraProjects, jiraFixVer, jiraFixVer) +
            " & updated <= '" + _jiraSearchDTF.format(Calendar.getInstance().getTime()) + "'", jiraFields));

        htmlReport += "</body>";

        return htmlReport;
    }

    /**
     * Search issues in jira using jql filter
     *
     * @param jql jql for search issues
     * @param fields issue fields name for return in response
     * @return List if issues implemented as JSONObject
     * @throws HttpException All error throws HttpException
     */
    private static List<JSONObject> searchIssues(final String jql, final String[] fields) throws HttpException {
        final List<JSONObject> issues = new CopyOnWriteArrayList<> ();
        final int startAt = 0;
        final int maxResults = 50;

         JSONObject firstRes = executeSearchRequest(jql, fields, startAt, maxResults);

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
                    JSONObject jiraRes = executeSearchRequest(jql, fields, maxResults * ost, maxResults);

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
    private static JSONObject executeSearchRequest(String jql, String[] fields, int startAt, int maxResults) throws HttpException {
        JSONObject data = new JSONObject();
        data.put("jql", jql);
        data.put("fields", fields);
        data.put("startAt", startAt);
        data.put("maxResults", maxResults);

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

    /**
     * Build reporting report css style string for HTML Page
     *
     * @return String with css formatted text
     */
    private static String buildReleaseReportCss() {
        return "<style>\n" +
                "h1 {\n" +
                "  color: #113847;\n" +
                "  font-size: 33px;\n" +
                "  font-weight: bold;\n" +
                "  margin: 30px 0 15px 0;\n" +
                "  padding-bottom: 7px;\n" +
                "  width: 700px;\n" +
                "}" +

                "h2 {" +
                "  border-bottom: 2px solid #ccc;\n" +
                "  color: #113847;\n" +
                "  font-size: 29px;\n" +
                "  font-weight: normal;\n" +
                "  margin: 30px 0 15px 0;\n" +
                "  padding-bottom: 7px;" +
                "  width: 700px;\n" +
                "}" +

                "a {\n" +
                "  color: #cc0000;\n" +
                "  text-decoration: none;\n" +
                "}\n" +

                "a:hover {\n" +
                "  text-decoration: underline;\n" +
                "}" +

                "ul,\n" +
                "ol {\n" +
                "  list-style: disc;\n" +
                "  margin-left: 30px;\n" +
                "}\n" +

                "ul li,\n" +
                "ol li {\n" +
                "  margin: 5px 0;\n" +
                "}\n" +
                "</style>\n";
    }

    /**
     *  Build report part for issues list
     *
     * @param hdr hdr for part
     * @param issues issues for add to part
     * @return HTML formatted string
     */
    private static String buildReportForIssueList(String hdr, List<JSONObject> issues) {
        String issuesReport = "";

        if (issues.size() > 0) {
            issuesReport = "<h2>" + hdr + "</h2>\n";

            issuesReport += "<ul>\n";

            for (JSONObject issue : issues) {
                String summary = issue.getJSONObject("fields").getString("summary");

                if (summary.lastIndexOf(".") == summary.length() - 1)
                    summary = summary.substring(0, summary.length() - 1);

                issuesReport += "<li>" + summary + "<a href=\"https://issues.apache.org/jira/browse/" + issue.getString("key") + "\"> [#" + issue.getString("key") + "]</a></li>\n";
            }

            issuesReport += "</ul>\n";
        }
        return issuesReport;
    }
}