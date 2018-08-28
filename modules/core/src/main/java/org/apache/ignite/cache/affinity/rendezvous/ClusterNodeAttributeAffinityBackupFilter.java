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
 * This class can be used as a {@link RendezvousAffinityFunction#affinityBackupFilter }.
 * It is constructed with a array of node attribute names, and a candidate node will be rejected if *any* of the 
 * already selected nodes have the identical values for *all* of those attributes on the candidate node.
 * 
 * For example, in AWS, you could configure each node with "availability_zone" attribute, and use this class, constructed 
 * with [ "availability_zone" ] as the {@link RendezvousAffinityFunction#affinityBackupFilter }.   This would only 
 * allow backup copies of a partition to be placed on nodes in different availability zones than the primary or other backups.
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
    * or {@code false} otherwise. An acceptable node is one where its set of attributes' values
    * is not exact match with any of the previously selected nodes.  If an attribute does not
    * exist on  either or both nodes, then the attribute does not match.
    *
    * @param candidate A node that is a candidate for becoming a backup node for a partition.
    * @param selected A list of primary/backup nodes already selected.  The primary is first.
    */
   @Override
   public boolean apply(ClusterNode candidate, List<ClusterNode> selected) {

      for (ClusterNode node : selected) {
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
