package org.elasticsearch.relay.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Base64;

public class HttpUtil {
	public static String getText(URL url) throws Exception {
		final StringBuffer buffer = new StringBuffer();

		HttpURLConnection connection = (HttpURLConnection) url.openConnection();

		// avoid Resteasy bug
		connection.setRequestProperty("Accept",
				"text/html,application/xhtml+xml,application/xml;" + "q=0.9,image/webp,*/*;q=0.8");

		// read response
		readResponse(connection, buffer);

		return buffer.toString();
	}

	public static String getAuthenticatedText(URL url, String user, String pass) throws Exception {
		final StringBuffer buffer = new StringBuffer();

		String login = user + ":" + pass;
		String encoded = Base64.encodeBase64String(login.getBytes());

		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestProperty("Authorization", "Basic " + encoded);

		// avoid Resteasy bug
		connection.setRequestProperty("Accept",
				"text/html,application/xhtml+xml,application/xml;" + "q=0.9,image/webp,*/*;q=0.8");

		// read response
		readResponse(connection, buffer);

		return buffer.toString();
	}

	public static String sendJson(URL url, String method, String data) throws Exception {
		final StringBuffer buffer = new StringBuffer();

		HttpURLConnection connection = (HttpURLConnection) url.openConnection();

		// send json data
		connection.setRequestMethod(method);
		
		if (data != null && data.length()>0) {
			byte[] bytes = data.getBytes("utf-8");
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setRequestProperty("Content-Length", String.valueOf(bytes.length));

			BufferedOutputStream writer = new BufferedOutputStream(connection.getOutputStream());
			writer.write(bytes);
			writer.flush();
			writer.close();
		}

		// read response
		readResponse(connection, buffer);

		return buffer.toString();
	}
	
	public static String sendForm(URL url, String method, String data) throws Exception {
		final StringBuffer buffer = new StringBuffer();

		HttpURLConnection connection = (HttpURLConnection) url.openConnection();

		// send json data
		connection.setRequestMethod(method);
		
		if (data != null && data.length()>0) {
			byte[] bytes = data.getBytes("utf-8");
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestProperty("Content-Type", "x-www-form-urlencoded");
			connection.setRequestProperty("Content-Length", String.valueOf(bytes.length));

			BufferedOutputStream writer = new BufferedOutputStream(connection.getOutputStream());
			writer.write(bytes);
			writer.flush();
			writer.close();
		}

		// read response
		readResponse(connection, buffer);

		return buffer.toString();
	}

	private static void readResponse(HttpURLConnection connection, StringBuffer buffer) throws Exception {
		BufferedReader reader = null;
		boolean error = false;
		int code = connection.getResponseCode();
		InputStream in ;
		if ( code == HttpURLConnection.HTTP_OK || code == HttpURLConnection.HTTP_CREATED || code == HttpURLConnection.HTTP_ACCEPTED ) {
			// normal response
			in = connection.getInputStream();
		} else  {
			// error response
			in = connection.getErrorStream();			
			error = true;
		}
		if(in==null){
			throw new Exception("Server error code " + connection.getResponseCode() + ": " + connection.getURL());
		}
		reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));

		String line = reader.readLine();
		while (line != null) {
			buffer.append(line);
			line = reader.readLine();
		}

		reader.close();

		if (error) {
			throw new Exception("Server error code " + connection.getResponseCode() + ": " + buffer.toString());
		}
	}
}
