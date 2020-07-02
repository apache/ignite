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

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;

import org.apache.ignite.internal.websession.WebSessionEntity;
import org.apache.ignite.marshaller.Marshaller;

/**
 * On session expiration, notify the attributes that they are being unbound from the session  
 * 
 * @param <K>
 * @param <V>
 */
public class WebSessionEntityCacheEntryExpiredListener<K, V> implements CacheEntryExpiredListener<String, WebSessionEntity>, Serializable {
    private static final long serialVersionUID = 1L;
    
    private final Marshaller marshaller;

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends String, ? extends WebSessionEntity>> events) throws CacheEntryListenerException {
        WebSessionEntityCacheEntryRemovedListener.notifyAttributes(events, marshaller);
    }

    public WebSessionEntityCacheEntryExpiredListener(Marshaller marshaller) {
        this.marshaller = marshaller;
    }
}
