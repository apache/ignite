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

import _ from 'lodash';
import pako from 'pako';
import bigIntJSON from 'json-bigint';

/** This worker decode & decompress BASE64/Zipped data and parse to JSON. */
// eslint-disable-next-line no-undef
onmessage = function(e) {
    const data = e.data;

    const binaryString = atob(data); // Decode from BASE64

    const unzipped = pako.inflate(binaryString, {to: 'string'});

    const res = bigIntJSON({storeAsString: true}).parse(unzipped);

    postMessage(_.get(res, 'result', res));
};
