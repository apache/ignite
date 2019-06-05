/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;
import org.h2.util.IOUtils;

/**
 * A simple web browser simulator.
 */
public class WebClient {

    private String sessionId;
    private String acceptLanguage;
    private String contentType;

    /**
     * Open an URL and get the HTML data.
     *
     * @param url the HTTP URL
     * @return the HTML as a string
     */
    String get(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        conn.setInstanceFollowRedirects(true);
        if (acceptLanguage != null) {
            conn.setRequestProperty("accept-language", acceptLanguage);
        }
        conn.connect();
        int code = conn.getResponseCode();
        contentType = conn.getContentType();
        if (code != HttpURLConnection.HTTP_OK) {
            throw new IOException("Result code: " + code);
        }
        InputStream in = conn.getInputStream();
        String result = IOUtils.readStringAndClose(new InputStreamReader(in), -1);
        conn.disconnect();
        return result;
    }

    /**
     * Upload a file.
     *
     * @param url the target URL
     * @param fileName the file name to post
     * @param in the input stream
     * @return the result
     */
    String upload(String url, String fileName, InputStream in) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setUseCaches(false);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Connection", "Keep-Alive");
        String boundary = UUID.randomUUID().toString();
        conn.setRequestProperty("Content-Type",
                "multipart/form-data;boundary="+boundary);
        conn.connect();
        DataOutputStream out = new DataOutputStream(conn.getOutputStream());
        out.writeBytes("--" + boundary + "--\r\n");
        out.writeBytes("Content-Disposition: form-data; name=\"upload\";"
                + " filename=\"" + fileName +"\"\r\n\r\n");
        IOUtils.copyAndCloseInput(in, out);
        out.writeBytes("\r\n--" + boundary + "--\r\n");
        out.close();
        int code = conn.getResponseCode();
        if (code != HttpURLConnection.HTTP_OK) {
            throw new IOException("Result code: " + code);
        }
        in = conn.getInputStream();
        String result = IOUtils.readStringAndClose(new InputStreamReader(in), -1);
        conn.disconnect();
        return result;
    }

    void setAcceptLanguage(String acceptLanguage) {
        this.acceptLanguage = acceptLanguage;
    }

    String getContentType() {
        return contentType;
    }

    /**
     * Read the session ID from a URL.
     *
     * @param url the URL
     * @return the session id
     */
    String readSessionId(String url) {
        int idx = url.indexOf("jsessionid=");
        String id = url.substring(idx + "jsessionid=".length());
        for (int i = 0; i < id.length(); i++) {
            char ch = id.charAt(i);
            if (!Character.isLetterOrDigit(ch)) {
                id = id.substring(0, i);
                break;
            }
        }
        this.sessionId = id;
        return id;
    }

    /**
     * Read the specified HTML page.
     *
     * @param url the base URL
     * @param page the page to read
     * @return the HTML page
     */
    String get(String url, String page) throws IOException {
        if (sessionId != null) {
            if (page.indexOf('?') < 0) {
                page += "?";
            } else {
                page += "&";
            }
            page += "jsessionid=" + sessionId;
        }
        if (!url.endsWith("/")) {
            url += "/";
        }
        url += page;
        return get(url);
    }

    /**
     * Get the base URL (the host name and port).
     *
     * @param url the complete URL
     * @return the host name and port
     */
    String getBaseUrl(String url) {
        int idx = url.indexOf("//");
        idx = url.indexOf('/', idx + 2);
        if (idx >= 0) {
            return url.substring(0, idx);
        }
        return url;
    }

}
