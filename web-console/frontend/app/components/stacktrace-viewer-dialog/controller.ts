/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Stacktrace } from 'app/types/index';

export default class {
    static $inject = ['title', 'stacktrace'];

    /** Title of stacktrace view dialog. */
    title: string;

    /** Stacktrace elements to show. */
    stacktrace: Stacktrace;

    /**
     * @param title Title of stacktrace view dialog.
     * @param stacktrace Stacktrace elements to show.
     */
    constructor(title: string, stacktrace: Stacktrace) {
        this.title = title;
        this.stacktrace = stacktrace;
    }
}
