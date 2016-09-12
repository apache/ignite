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

import angular from 'angular';

import IgniteBranding from './branding.provider';

import igniteHeaderLogo from './header-logo.directive';
import igniteHeaderTitle from './header-title.directive';
import igniteTerms from './terms.directive';
import igniteFeatures from './features.directive';
import igniteFooter from './footer.directive';
import ignitePoweredByApache from './powered-by-apache.directive';

angular
.module('ignite-console.branding', [
    'ui.router.metatags'
])
.provider(...IgniteBranding)
.config(['UIRouterMetatagsProvider', (UIRouterMetatagsProvider) => {
    UIRouterMetatagsProvider
        .setDefaultTitle('Apache Ignite - Management Tool and Configuration Wizard')
        .setTitleSuffix(' – Apache Ignite Web Console')
        .setDefaultDescription('The Apache Ignite Web Console is an interactive management tool and configuration wizard which walks you through the creation of config files. Try it now.');
}])
.directive(...ignitePoweredByApache)
.directive(...igniteHeaderLogo)
.directive(...igniteHeaderTitle)
.directive(...igniteTerms)
.directive(...igniteFeatures)
.directive(...igniteFooter);
