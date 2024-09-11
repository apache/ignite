/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
