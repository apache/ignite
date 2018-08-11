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

package org.apache.ignite.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.IgniteStripedThreadPoolExecutorMXBean;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.mxbean.StripedExecutorMXBean;
import org.apache.ignite.mxbean.ThreadPoolMXBean;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;

/**
 * Adapter for {@link IgniteStripedThreadPoolExecutorMXBean} which delegates all method calls to the underlying
 * {@link ExecutorService} instance.
 */
public class IgniteStripedThreadPoolExecutorMXBeanAdapter implements IgniteStripedThreadPoolExecutorMXBean {
    /** */
    private final IgniteStripedThreadPoolExecutor exec;

    /**
     * @param exec Executor service
     */
    IgniteStripedThreadPoolExecutorMXBeanAdapter(IgniteStripedThreadPoolExecutor exec) {
        assert exec != null;

        this.exec = exec;
    }

    /** {@inheritDoc} */
    @Override public int getStripesCount() {
        return exec.stripes();
    }

    /** {@inheritDoc} */
    @Override public int getActiveCount() {
       int count = 0;
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          count += a.getActiveCount();
       }

       return count;
    }

    /** {@inheritDoc} */
    @Override public long getCompletedTaskCount() {
       int count = 0;
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          count += a.getCompletedTaskCount();
       }

       return count;
    }

    /** {@inheritDoc} */
    @Override public int getCorePoolSize() {
       int count = 0;
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          count += a.getCorePoolSize();
       }

       return count;
    }

    /** {@inheritDoc} */
    @Override public int getLargestPoolSize() {
       int count = 0;
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
         
          if (count <  a.getLargestPoolSize()) {
             count = a.getLargestPoolSize();
          }
       }

       return count;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumPoolSize() {
       int count = 0;
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          if (count <  a.getMaximumPoolSize()) {
             count = a.getMaximumPoolSize();
          }
       }

       return count;
    }

    /** {@inheritDoc} */
    @Override public int getPoolSize() {
       int count = 0;
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          count += a.getPoolSize();
       }

       return count;
    }

    /** {@inheritDoc} */
    @Override public long getTaskCount() {
       int count = 0;
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          count += a.getTaskCount();
       }

       return count;
    }

    /** {@inheritDoc} */
    @Override public int getQueueSize() {
       int count = 0;
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          count += a.getQueueSize();
       }

       return count;
    }

    /** {@inheritDoc} */
    @Override public long getKeepAliveTime() {
       return new ThreadPoolMXBeanAdapter(exec.stripe(0)).getKeepAliveTime();
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          if (!a.isShutdown()) {
             return false;         
          }
       }
       return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          if (!a.isTerminated()) {
             return false;         
          }
       }
       return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminating() {
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          if (a.isTerminating()) {
             return true;         
          }
       }

       return false;
    }

    /** {@inheritDoc} */
    @Override public String getRejectedExecutionHandlerClass() {
       return new ThreadPoolMXBeanAdapter(exec.stripe(0)).getRejectedExecutionHandlerClass();
    }

    /** {@inheritDoc} */
    @Override public String getThreadFactoryClass() {
       return new ThreadPoolMXBeanAdapter(exec.stripe(0)).getThreadFactoryClass();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteStripedThreadPoolExecutorMXBeanAdapter.class, this, super.toString());
    }
    
    @MXBeanDescription("Number of completed tasks per stripe.")
    public long[] getStripesCompletedTasksCounts() {
       long [] result = new long[exec.stripes()];
       
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          result[i] = a.getCompletedTaskCount();
       }

       return result;
    }

    /**
     * @return Number of active tasks per stripe.
     */
    @MXBeanDescription("Number of active tasks per stripe.")
    public int[] getStripesActiveCount() {
       int [] result = new int[exec.stripes()];
       
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          result[i] = a.getActiveCount();
       }

       return result;
    }

    /**
     * @return Size of queue per stripe.
     */
    @MXBeanDescription("Size of queue per stripe.")
    public int[] getStripesQueueSizes() {
       int [] result = new int[exec.stripes()];
       
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          result[i] = a.getQueueSize();
       }

       return result;
    }
    
    /**
     * @return Size of queue per stripe.
     */
    @MXBeanDescription("Size of queue per stripe.")
    public int[] getStripesLargestPoolSize() {
       int [] result = new int[exec.stripes()];
       
       for (int i = 0; i < exec.stripes(); i++) {
          ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
          
          result[i] = a.getLargestPoolSize();
       }

       return result;
    }

   @Override
   public boolean[] getStripesActiveStatuses() {
      boolean [] result = new boolean[exec.stripes()];
      
      for (int i = 0; i < exec.stripes(); i++) {
         ThreadPoolMXBean a = new ThreadPoolMXBeanAdapter(exec.stripe(i));
         
         result[i] = a.getActiveCount() > 0 ;
      }

      return result;
   }
}

