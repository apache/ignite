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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObjectSerializer;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteNodeFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_21_0;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class TestCommandResponseSerializer implements IgniteDataTransferObjectSerializer<TestCommandResponse> {
    /** {@inheritDoc} */
    @Override public void writeExternal(
        TestCommandResponse obj,
        ObjectOutput out,
        MessageSerializationContext ctx
    ) throws IOException {
        out.writeObject(obj.jobFeatures);
        out.writeObject(obj.taskFeatures);
        U.writeString(out, obj.fldA);
        U.writeString(out, obj.fldB);

        if (ctx.includeFieldDeprecatedBy(TestIgniteReleaseFeatures_2_21_0.VER_2_21_0_ID_5_FEATURE))
            U.writeString(out, obj.fldC);

        if (ctx.includeFieldIntroducedBy(TestIgniteReleaseFeatures_2_21_0.VER_2_21_0_ID_5_FEATURE))
            U.writeString(out, obj.fldD);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(
        TestCommandResponse obj,
        ObjectInput in,
        MessageSerializationContext ctx
    ) throws IOException, ClassNotFoundException {
        obj.jobFeatures = (IgniteNodeFeatureSet)in.readObject();
        obj.taskFeatures = (IgniteNodeFeatureSet)in.readObject();
        obj.fldA = U.readString(in);
        obj.fldB = U.readString(in);

        if (ctx.includeFieldDeprecatedBy(TestIgniteReleaseFeatures_2_21_0.VER_2_21_0_ID_5_FEATURE))
            obj.fldC = U.readString(in);

        if (ctx.includeFieldIntroducedBy(TestIgniteReleaseFeatures_2_21_0.VER_2_21_0_ID_5_FEATURE))
            obj.fldD = U.readString(in);
    }
}
