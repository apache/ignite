package mock;

import org.apache.lucene.document.FloatPoint;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.Arrays;

public class FloatPointTest {
	
	@Test
	public void testFloatSorted() {
		float[] f = {-10.0f, -1.22f, -1.2f, 0.0f, 1.0f, 1.2f, 9.09f, 9.9f};
		float[] f2 = new float[f.length];
		byte[][] dests = new byte[f.length][4];
		for(int i=0;i<f.length;i++) {
			byte [] dest = new byte[4];
			dests[i] = dest;
			FloatPoint.encodeDimension(f[i], dest, 0);
			
			f2[i] = FloatPoint.decodeDimension(dest, 0);
		}
		
		for(int i=0;i<f.length-1;i++) {
			int ret = Arrays.compareUnsigned(dests[i], dests[i+1]);
			
			System.out.println("<"+ret);
		}
		
		assertArrayEquals("encode-decode",f,f2,Float.MIN_VALUE);
		
	}

}
