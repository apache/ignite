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

import angular from 'angular';

import IgniteBranding from './branding.service';

import igniteTerms from './terms.directive';
import igniteFeatures from './features.directive';

angular
.module('ignite-console.branding', [
    'tf.metatags'
])
.service('IgniteBranding', IgniteBranding)
.config(['tfMetaTagsProvider', (tfMetaTagsProvider) => {
    tfMetaTagsProvider.setDefaults({
        title: 'Apache Ignite - Management Tool and Configuration Wizard',
        properties: {
            description: 'The Apache Ignite Web Console is an interactive management tool and configuration wizard which walks you through the creation of config files. Try it now.'
        }
    });

    tfMetaTagsProvider.setTitleSuffix(' – Apache Ignite Web Console');
}])
.directive('igniteTerms', igniteTerms)
.directive('igniteFeatures', igniteFeatures);
