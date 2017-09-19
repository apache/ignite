package org.apache.ignite.internal.processors.cluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * This class is responsible for getting Ignite updates information via HTTP
 */
public class HttpIgniteUpdatesChecker {
    /** Url for request updates. */
    private final String url;

    /** Charset for encoding requests/responses */
    private final String charset;

    /**
     * Creates new HTTP Ignite updates checker with following parameters
     * @param url URL for getting Ignite updates information
     * @param charset Charset for encoding
     */
    HttpIgniteUpdatesChecker(String url, String charset) {
        this.url = url;
        this.charset = charset;
    }

    /**
     * Gets information about Ignite updates via HTTP
     * @param updateRequest HTTP Request parameters
     * @return Information about Ignite updates separated by line endings
     * @throws IOException If HTTP request was failed
     */
    public String getUpdates(String updateRequest) throws IOException {
        URLConnection conn = new URL(url).openConnection();
        conn.setDoOutput(true);
        conn.setRequestProperty("Accept-Charset", charset);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + charset);

        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(updateRequest.getBytes(charset));
        }

        try (InputStream in = conn.getInputStream()) {
            if (in == null)
                return null;

            BufferedReader reader = new BufferedReader(new InputStreamReader(in, charset));

            StringBuilder response = new StringBuilder();

            for (String line; (line = reader.readLine()) != null; ) {
                response.append(line).append('\n');
            }

            return response.toString();
        }
    }
}
