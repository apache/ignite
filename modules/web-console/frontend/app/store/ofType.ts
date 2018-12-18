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

import {OperatorFunction} from 'rxjs/interfaces';
import {filter} from 'rxjs/operators';

type Action = {type: string};
export function ofType<T extends string, U extends Action, V extends Extract<U, {type: T}>>(type: T): OperatorFunction<U, V>
export function ofType<U extends Action>(type): OperatorFunction<U, U> {
    return filter((action: U): boolean => type === action.type);
}

