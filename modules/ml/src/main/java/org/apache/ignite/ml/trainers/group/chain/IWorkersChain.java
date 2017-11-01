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

package org.apache.ignite.ml.trainers.group.chain;

//@FunctionalInterface
public interface IWorkersChain<I, C, O> extends BaseWorkersChain<I, C, O> {
    default IWorkersChain<I, C, O> embed(O val) {
        return (input, context) -> val;
    }

    default <C1, O1> IWorkersChain<I, C, O1> withOtherContext(IWorkersChain<O, C1, O1> newChain, C1 otherContext) {
        return (input, context) -> {
            O res = process(input, context);
            return newChain.process(res, otherContext);
        };
    }


    default <O1, T extends IWorkersChain<O, C, O1>> IWorkersChain<I, C, O1> then(T next) {
        IWorkersChain<I, C, O> me = this;
        return (input, context) -> {
            O myRes = me.process(input, context);
            return next.process(myRes, context);
        };
    }
}
