

package org.apache.ignite.console.utils;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.ignite.spi.IgniteSpiException;
import org.junit.Test;


/**
 * Test for Web Console utils.
 */
public class UtilsTest {
    /** */
    @Test
    public void testDummy() {
        System.out.println("Dummy test");
    }
    
    @Test
    public void testURL()  {
    	
    	String masterURL = "file:///ignite-web-console/work";
		try {
			URI uri = new URI(masterURL);  
            // 对于file协议，getSchemeSpecificPart()通常会返回路径部分，但注意它可能包含?和#  
            // 对于简单的file://路径，我们可以直接用它，但最好先检查  
            String path = uri.getSchemeSpecificPart().replaceFirst("^/", ""); // 如果URI以/开头，则去除它  
            File fileFromURI = new File(path);  
			System.out.println("Dummy testURL "+fileFromURI);
		} catch (Exception e) {				
			throw new IgniteSpiException(e);
		}
    }
}
