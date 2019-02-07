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

export let devTools;

const replacer = (key, value) => {
    if (value instanceof Map) {
        return {
            data: [...value.entries()],
            __serializedType__: 'Map'
        };
    }
    if (value instanceof Set) {
        return {
            data: [...value.values()],
            __serializedType__: 'Set'
        };
    }
    if (value instanceof Symbol) {
        return {
            data: String(value),
            __serializedType__: 'Symbol'
        };
    }
    if (value instanceof Promise) {
        return {
            data: {}
        };
    }
    return value;
};

const reviver = (key, value) => {
    if (typeof value === 'object' && value !== null && '__serializedType__' in value) {
        const data = value.data;
        switch (value.__serializedType__) {
            case 'Map':
                return new Map(value.data);
            case 'Set':
                return new Set(value.data);
            default:
                return data;
        }
    }
    return value;
};

if (window.__REDUX_DEVTOOLS_EXTENSION__) {
    devTools = window.__REDUX_DEVTOOLS_EXTENSION__.connect({
        name: 'Ignite Web Console',
        serialize: {
            replacer,
            reviver
        }
    });
}

export const reducer = (state, action) => {
    switch (action.type) {
        case 'DISPATCH':
        case 'JUMP_TO_STATE':
            return JSON.parse(action.state, reviver);
        default:
            return state;
    }
};
