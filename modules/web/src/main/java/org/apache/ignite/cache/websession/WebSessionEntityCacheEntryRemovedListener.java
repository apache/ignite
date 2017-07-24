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
import java.util.Map;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;

import org.apache.ignite.internal.websession.WebSessionEntity;
import org.apache.ignite.marshaller.Marshaller;

/**
 * On session being removed from the cache, notify its attributes that they are being unbound from the session
 * 
 * @param <K>
 * @param <V>
 */
public class WebSessionEntityCacheEntryRemovedListener<K, V> implements CacheEntryRemovedListener<String, WebSessionEntity>, Serializable {
    private static final long serialVersionUID = 1L;
    
    private final Marshaller marshaller;
    
    public static void notifyAttributes(Iterable<CacheEntryEvent<? extends String, ? extends WebSessionEntity>> events, final Marshaller marshaller) {
        if (events == null) {
            return;
        }
        
        events.forEach((e)->{
            WebSessionEntity webSessionEntity = e.getOldValue();
            if (webSessionEntity == null) {
                return;
            }
            
            WebSessionV2 webSessionV2 = new WebSessionV2(webSessionEntity, marshaller);            
            Map<String, byte[]> attrs = webSessionEntity.attributes();
            if (attrs == null) {
                return;
            }
            
            attrs.forEach((name, value) -> {
                webSessionV2.notifyAttributeBeingUnbound(name);
            });
        });
    }

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends String, ? extends WebSessionEntity>> events) throws CacheEntryListenerException {
        notifyAttributes(events, marshaller);
    }

    public WebSessionEntityCacheEntryRemovedListener(final Marshaller marshaller) {
        this.marshaller = marshaller;
    }    
}
