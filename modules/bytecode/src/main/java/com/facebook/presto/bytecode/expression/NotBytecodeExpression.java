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

package com.facebook.presto.bytecode.expression;

import java.util.List;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.instruction.LabelNode;

import static com.facebook.presto.bytecode.BytecodeUtils.checkArgument;
import static com.facebook.presto.bytecode.ParameterizedType.type;

class NotBytecodeExpression
        extends BytecodeExpression
{
    private final BytecodeExpression value;

    NotBytecodeExpression(BytecodeExpression value)
    {
        super(type(boolean.class));
        this.value = value;
        checkArgument(value.getType().getPrimitiveType() == boolean.class, "Expected value to be type boolean but is %s", value.getType());
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        LabelNode trueLabel = new LabelNode("true");
        LabelNode endLabel = new LabelNode("end");
        return new BytecodeBlock()
                .append(value)
                .ifTrueGoto(trueLabel)
                .push(true)
                .gotoLabel(endLabel)
                .visitLabel(trueLabel)
                .push(false)
                .visitLabel(endLabel);
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return List.of(value);
    }

    @Override
    protected String formatOneLine()
    {
        return "(!" + value + ")";
    }
}
