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

 package org.apache.ignite.internal.processors.service;

 import java.util.Collection;
 import java.util.EventListener;
 import java.util.List;
 import java.util.concurrent.ConcurrentMap;
 import org.apache.ignite.internal.IgniteEx;
 import org.apache.ignite.internal.events.DiscoveryCustomEvent;
 import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
 import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
 import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
 import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;
 import org.apache.ignite.testframework.GridTestUtils;
 import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
 import org.junit.Assume;
 import org.junit.Before;
 import org.junit.Test;

 /**
  * <b>Tests in the class strongly depend on implementation of {@link GridEventStorageManager} and internal logic of
  * services and PME discovery listeners.</b>
  * <p/>
  * Tests that discovery listener registered by {@link ServiceDeploymentManager} will be notified earlier than discovery
  * listener registered by {@link GridCachePartitionExchangeManager}. It allows service manager capture custom message
  * because it may be nullified in PME process at the end of exchange in {@link GridDhtPartitionsExchangeFuture#onDone()}.
  */
 public class ServiceDeploymentDiscoveryListenerNotificationOrderTest extends GridCommonAbstractTest {
     /** */
     @Before
     public void check() {
         Assume.assumeTrue(isEventDrivenServiceProcessorEnabled());
     }

     /**
      * <b>Strongly depends on internal implementation of {@link GridEventStorageManager}.</b>
      * <p/>
      * Tests that discovery listener registered by {@link ServiceDeploymentManager} is in collection of hight priority
      * listeners, at the same time discovery listener registered by {@link GridCachePartitionExchangeManager} is in
      * collection of usual listeners.
      * <p/>
      * This guarantees that service deployment discovery listener will be notified earlier that PME's discovery listener
      * and will be able to capture custom messages which may be nullified in PME process.
      *
      * @throws Exception In case of an error.
      */
     @Test
     public void testServiceDiscoveryListenerNotifiedEarlierThanPME() throws Exception {
         try {
             IgniteEx ignite = startGrid(0);

             ConcurrentMap<Integer, Object> lsnrs = GridTestUtils
                 .getFieldValue(ignite.context().event(), "lsnrs");

             Object customLsnrs = lsnrs.get(DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT);

             List<EventListener> highPriorityLsnrs = GridTestUtils.getFieldValue(customLsnrs, "highPriorityLsnrs");

             GridConcurrentLinkedHashSet<EventListener> usualLsnrs = GridTestUtils
                 .getFieldValue(customLsnrs, "lsnrs");

             assertTrue("Failed to find service deployment manager's listener in high priority listeners.",
                 containsListener(highPriorityLsnrs, ServiceDeploymentManager.class));

             assertFalse("Service deployment manager's listener shoud not be registered in usual listeners.",
                 containsListener(usualLsnrs, ServiceDeploymentManager.class));

             assertTrue("Failed to find PME manager's discovery listener in usual listeners.",
                 containsListener(usualLsnrs, GridCachePartitionExchangeManager.class));

             assertFalse("PME manager's discovery listener shoud not be registered in high priority listeners.",
                 containsListener(highPriorityLsnrs, GridCachePartitionExchangeManager.class));
         }
         finally {
             stopAllGrids();
         }
     }

     /**
      * @param lsnrs Collection of listeners.
      * @param cls Listener class.
      * @return {@code true} if given collection contains expected listener, otherwise {@code false}.
      */
     private boolean containsListener(Collection<EventListener> lsnrs, Class cls) {
         for (Object wrapper : lsnrs) {
             Object lsnr = GridTestUtils.getFieldValue(wrapper, "lsnr");

             // We can't use 'instance of' or other reflection tools to check the type of class, because, service
             // deployment discovery listener is a private non-static class, PME's listener is an anonymous class.
             if (lsnr.getClass().getName().startsWith(cls.getName()))
                 return true;
         }

         return false;
     }
 }
