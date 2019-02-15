/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
