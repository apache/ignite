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

 import org.apache.ignite.internal.MessageSerializationContext;
 import org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry;
 import java.io.ObjectOutput;
 import org.apache.ignite.internal.util.typedef.internal.U;
 import org.apache.ignite.internal.TestIgniteDataTransferObject;
 import java.io.ObjectInput;
 import java.io.IOException;
 import org.apache.ignite.internal.dto.IgniteDataTransferObjectSerializer;

 /**
  * This class is generated automatically.
  *
  * @see org.apache.ignite.internal.dto.IgniteDataTransferObject
  */
 public class TestIgniteDataTransferObjectSerializer implements IgniteDataTransferObjectSerializer<TestIgniteDataTransferObject> {
     /** {@inheritDoc} */
     @Override public void writeExternal(TestIgniteDataTransferObject obj, ObjectOutput out, MessageSerializationContext ctx) throws IOException {
         U.writeCharArray(out, obj.charArray);
          if (ctx.includeFieldDeprecatedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE)) {
             U.writeString(out, obj.deprecatedFld);
         }
         if (ctx.includeFieldIntroducedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE)) {
             U.writeString(out, obj.introducedFld);
         }
     }

     /** {@inheritDoc} */
     @Override public void readExternal(TestIgniteDataTransferObject obj, ObjectInput in, MessageSerializationContext ctx) throws IOException, ClassNotFoundException {
         obj.charArray = U.readCharArray(in);
          if (ctx.includeFieldDeprecatedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE)) {
             obj.deprecatedFld = U.readString(in);
         }
         if (ctx.includeFieldIntroducedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE)) {
             obj.introducedFld = U.readString(in);
         }
     }
 }
