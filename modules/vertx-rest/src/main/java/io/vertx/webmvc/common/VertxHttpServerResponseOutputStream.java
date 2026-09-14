package io.vertx.webmvc.common;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.io.IOException;
import java.io.OutputStream;

public class VertxHttpServerResponseOutputStream extends OutputStream {
    private final HttpServerResponse httpServerResponse;
    private Buffer buffer = Buffer.buffer(1024);
    private int maxBufferSize = 1024*1024; // 1M
    
    public VertxHttpServerResponseOutputStream(HttpServerResponse httpServerResponse) {
        this.httpServerResponse = httpServerResponse;
    }

    @Override
    public void write(int b) throws IOException {
        // 将单个字节写入响应
        buffer.appendByte((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        // 将字节数组写入响应
    	if(buffer.length()>maxBufferSize) {
    		this.flush();
    	}
    	buffer.appendBytes(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // 将字节数组的一部分写入响应
    	if(buffer.length()>maxBufferSize) {
    		this.flush();
    	}
    	buffer.appendBytes(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        // 刷新输出
    	httpServerResponse.write(buffer);
    	buffer = Buffer.buffer();    	
    }

    @Override
    public void close() throws IOException {
        // 关闭响应，实际应用中可能需要更复杂的处理
        httpServerResponse.end(buffer);   
    }
}