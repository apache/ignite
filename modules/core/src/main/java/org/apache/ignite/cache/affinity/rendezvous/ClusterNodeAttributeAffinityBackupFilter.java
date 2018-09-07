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

package org.apache.ignite.cache.affinity.rendezvous;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * This class can be used as a {@link RendezvousAffinityFunction#affinityBackupFilter } to create 
 * cache templates in Spring that force each partition's primary and backup to different hardware which 
 * is not expected to fail simultaneously, e.g., in AWS, to different "availability zones".  This
 * is a per-partition selection, and different partitions may choose different primaries.
 * 
 * This implementation will discard backups rather than place multiple on the same set of nodes. This avoids
 * trying to cram more data onto remaining nodes  when some have failed.
 * 
 * A list of node attributes to compare is provided on construction.  Note: "All cluster nodes, 
 * on startup, automatically register all the environment and system properties as node attributes."
 *  
 * This class is constructed with a array of node attribute names, and a candidate node will be rejected if *any* of the 
 * previously selected nodes for a partition have the identical values for *all* of those attributes on the candidate node.  
 * 
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * Create a partitioned cache template plate with 1 backup, where the backup will not be placed in the same availability zone
 * as the primary.   Note: This example requires that the environment variable "AVAILABILTY_ZONE" be set appropriately on 
 * each node via some means external to Ignite.  On AWS, some nodes might have AVAILABILTY_ZONE=us-east-1a and others 
 * AVAILABILTY_ZONE=us-east-1b. 
 * <pre name="code" class="xml">
 * 
 * &lt;property name="cacheConfiguration"&gt; 
 *     &lt;list&gt; 
 *         &lt;bean id="cache-template-bean" abstract="true" class="org.apache.ignite.configuration.CacheConfiguration"&gt; 
 *             &lt;property name="name" value="JobcaseDefaultCacheConfig*"/&gt; 
 *             &lt;property name="cacheMode" value="PARTITIONED" /&gt; 
 *             &lt;property name="backups" value="1" /&gt; 
 *             &lt;property name="affinity"&gt;
 *                 &lt;bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction"&gt;
 *                     &lt;property name="affinityBackupFilter"&gt;
 *                         &lt;bean class="org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter"&gt;
 *                            &lt;constructor-arg&gt;
 *                                &lt;array value-type="java.lang.String"&gt;
 *                                     &lt;!-- Backups must go to different AZs --&gt;
 *                                     &lt;value&gt;AVAILABILITY_ZONE&lt;/value&gt;
 *                                &lt;/array&gt;
 *                            &lt;/constructor-arg&gt;                                   
 *                         &lt;/bean&gt;
 *                     &lt;/property&gt;
 *                &lt;/bean&gt;
 *             &lt;/property&gt;
 *        &lt;/bean&gt; 
 *    &lt;/list&gt; 
 *  &lt;/property&gt; 
 * </pre>
 * 
 * With more backups, multiple properties, e.g., SITE, ZONE,  could be used to force backups to different subgroups. 
 */
public class ClusterNodeAttributeAffinityBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>>, Serializable {
   /**
    * 
    */
   private static final long serialVersionUID = 1L;

   
   final String [] attributeNames;

   /*
    * @param attributeNames - the list of attribute names for the set of attributes to compare. Must be at least one.
    */
   ClusterNodeAttributeAffinityBackupFilter(String[] attributeNames)
   {
      assert attributeNames.length > 0;
      
      this.attributeNames = attributeNames;
   }

   /**
    * Defines a predicate which returns {@code true} if a node is acceptable for a backup
    * or {@code false} otherwise. An acceptable node is one where its set of attribute values
    * is not exact match with any of the previously selected nodes.  If an attribute does not
    * exist on  either or both nodes, then the attribute does not match.
    *
    * @param candidate A node that is a candidate for becoming a backup node for a partition.
    * @param previouslySelected A list of primary/backup nodes already chosen for a partition.  
    * The primary is first.
    */
   @Override
   public boolean apply(ClusterNode candidate, List<ClusterNode> previouslySelected) {

      for (ClusterNode node : previouslySelected) {
         boolean match = true;
         
         for (String attribute : attributeNames) {
            Object candidateAttrValue = candidate.attribute(attribute);
            

            if (candidateAttrValue == null) {
               match = false;
               break;
            }
            else {
               Object nodeAttributeValue = node.attribute(attribute); 
               if ( nodeAttributeValue == null || !Objects.equals(candidateAttrValue, nodeAttributeValue) ) {
                  match = false;
                  break;
               }
            }
         }
         if (match) {
            return false;
         }    
      }
      return true;
   }

}
