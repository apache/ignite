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

package org.apache.ignite.cache.websession;

import java.io.Serializable;
import java.util.Enumeration;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;

/**
 * On session being removed from the cache, notify its attributes that they are being unbound from the session
 * 
 * @param <K>
 * @param <V>
 */
public class WebSessionCacheEntryRemovedListener<K, V> implements CacheEntryRemovedListener<String, WebSession>, Serializable {
    private static final long serialVersionUID = 1L;
    
    public static void notifyAttributes(Iterable<CacheEntryEvent<? extends String, ? extends WebSession>> events) {
        if (events == null) {
            return;
        }
        
        events.forEach((e)->{
            WebSession webSession = e.getOldValue();
            if (webSession == null) {
                return;
            }
            
            Enumeration<String> attrNames = webSession.getAttributeNames();
            if (attrNames == null) {
                return;
            }
            
            while (attrNames.hasMoreElements()) {
                String name = attrNames.nextElement();
                webSession.notifyAttributeBeingUnbound(name);
            }            
        });
    }

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends String, ? extends WebSession>> events) throws CacheEntryListenerException {
        notifyAttributes(events);
    }
}
