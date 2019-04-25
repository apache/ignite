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

import {Selector} from 'testcafe';
import {FormField} from '../components/FormField';
import {isVisible} from '../helpers';

export const createModelButton = Selector('pc-items-table footer-slot .link-success').filter(isVisible);
export const general = {
    generatePOJOClasses: new FormField({id: 'generatePojoInput'}),
    queryMetadata: new FormField({id: 'queryMetadataInput'}),
    keyType: new FormField({id: 'keyTypeInput'}),
    valueType: new FormField({id: 'valueTypeInput'})
};
