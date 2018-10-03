package org.apache.ignite.spi.discovery.tcp.ipfinder.consul.util;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author rjain
 */
public class PublicIpService {
    private static final List<String> publicIpServices = Arrays.asList("https://api.ipify.org");//,"https://ipapi.co/ip");
    private static HttpClient httpClient = HttpClientBuilder.create().build();

    private PublicIpService() {
    }

    /**
     * Gets the public ip address through ipify's api. By default, uses
     * an http connection (which is about twice as fast as an https connection).
     *
     * @return The public ip address.
     * @throws IOException If there is an IO error.
     */
    public static String getPublicIp() throws IOException {
        return getPublicIp(false);
    }

    /**
     * Gets the public ip address through ipify's api.
     *
     * @param useHttps If true, will use an https connection. If false, will use http.
     * @return The public ip address.
     * @throws IOException If there is an IO error.
     */
    public static String getPublicIp(boolean useHttps) throws IOException {
        String hostUrl = publicIpServices.get(Math.min((int) (System.currentTimeMillis() % 10), publicIpServices.size() - 1));
        String ip = httpClient.execute(new HttpGet(hostUrl)).getEntity().getContent().toString();
        return ip;
    }

}