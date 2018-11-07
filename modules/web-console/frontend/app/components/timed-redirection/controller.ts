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

import {StateOrName} from '@uirouter/core/lib/state/interface';
import {RawParams} from '@uirouter/core/lib/params/interface';

interface State {
    name: StateOrName,
    params: RawParams
}

export class TimedRedirectionCtrl implements ng.IComponentController {
    static $inject = ['$state'];

    lastSuccessState = JSON.parse(localStorage.getItem('lastStateChangeSuccess'));

    stateToGo: State = this.lastSuccessState || {name: 'default-state', params: {}};

    urlToGo: string = this.$state.href(this.stateToGo.name, this.stateToGo.params);

    constructor(private $state) {}

    go() {
        this.$state.go(this.stateToGo.name, this.stateToGo.params);
    }
}
