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

import browserUpdate from 'browser-update';
import './style.scss';

browserUpdate({
    notify: {
        i: 11,
        f: '-18m',
        s: 9,
        c: '-18m',
        o: '-18m',
        e: '-6m'
    },
    l: 'en',
    mobile: false,
    api: 5,
    // This should work in older browsers
    text: '<b>Outdated or unsupported browser detected.</b> Web Console may work incorrectly. Please update to one of modern fully supported browsers! <a {up_but}>Update</a> <a {ignore_but}>Ignore</a>',
    reminder: 0
});
