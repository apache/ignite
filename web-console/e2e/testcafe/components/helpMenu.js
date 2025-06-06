/*
 * Copyright 2019 Ignite Systems, Inc. and Contributors.
 *
 * Licensed under the Ignite Community Edition License (the "License");
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

export const trigger = Selector('help-menu > .help-menu__trigger');
export const item = (label) => trigger.find('[role="menuitem"]').withText(label);
