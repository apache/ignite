/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

public class IgniteSpringBeanTest {
    /** Test checkIgnite does not throws IllegalStateException.  */
    @Test public void testCheckIgnite() throws Exception {
        IgniteSpringBean igniteSpringBean = new IgniteSpringBean();
        igniteSpringBean.afterPropertiesSet();
        try{
        igniteSpringBean.checkIgnite();
        }catch (IllegalStateException e) {
            fail("Failed as Ignite should have been initialized");
        }
    }

    /** Test checkIgnite throws IllegalStateException.  */
    @Test public void testCheckIgniteThrowsException() throws Exception {
        IgniteSpringBean igniteSpringBean = new IgniteSpringBean();
        try{
            igniteSpringBean.checkIgnite();
            fail("Failed as Ignite should not have been initialized");
        }catch (Exception e){
            assertTrue(e instanceof IllegalStateException);
        }
    }
}
