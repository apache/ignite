package de.kp.works.janus;

import java.nio.ByteBuffer;

public class IgniteValue {	
	private String s;
	//private ByteBuffer b;
	private byte[] data;
	

	public IgniteValue(String s) {
		this.s = s;
	}
	// read data from bytebuffer
	public IgniteValue(ByteBuffer byteBuffer) {
		final int remaining = byteBuffer.remaining();
        // Use the underlying buffer if possible
        if (byteBuffer.hasArray()) {
            final byte[] byteArray = byteBuffer.array();
            if (remaining == byteArray.length) {
                byteBuffer.position(remaining);
                data = byteArray;
                return ;
            }
        }
        // Copy the bytes
        final byte[] byteArray = new byte[remaining];
        byteBuffer.get(byteArray);
        data =  byteArray;
	}
	
	public IgniteValue(byte[] data) {
		this.data = data;
	}

	public String getS() {
		return s;
	}
	
	//public ByteBuffer getB() {
	//	return b;
	//}
	
	public byte[] data() {
		return data;
	}
	
}
