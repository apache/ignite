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

import _ from 'lodash';
import pako from 'pako';
import bigIntJSON from 'json-bigint';

/** This worker decode & decompress BASE64/Zipped data and parse to JSON. */
// eslint-disable-next-line no-undef
onmessage = function(e) {
    const data = e.data;

    const binaryString = atob(data.payload); // Decode from BASE64

    const unzipped = pako.inflate(binaryString, {to: 'string'});

    const res = data.useBigIntJson
        ? bigIntJSON({storeAsString: true}).parse(unzipped)
        : JSON.parse(unzipped);

    postMessage(_.get(res, 'result', res));
};
