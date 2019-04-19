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

import {StateOrName, StateService} from '@uirouter/angularjs';
import {RawParams} from '@uirouter/core/lib/params/interface';
import {default as UserFactory} from 'app/modules/user/User.service';

interface State {
    name: StateOrName,
    params: RawParams
}

export class TimedRedirectionCtrl implements ng.IComponentController, ng.IOnInit, ng.IOnDestroy {
    static $inject = ['$state', '$interval', 'User'];

    lastSuccessState = JSON.parse(localStorage.getItem('lastStateChangeSuccess'));

    stateToGo: State = this.lastSuccessState || {name: 'default-state', params: {}};

    secondsLeft: number = 10;

    countDown: ng.IPromise<ng.IIntervalService>;

    constructor(private $state: StateService, private $interval: ng.IIntervalService, private user: ReturnType<typeof UserFactory>) {}

    $onInit() {
        this.startCountDown();
    }

    $onDestroy() {
        this.$interval.cancel(this.countDown);
    }

    async go(): void {
        try {
            await this.user.load();

            this.$state.go(this.stateToGo.name, this.stateToGo.params);
        }
        catch (ignored) {
            this.$state.go('signin');
        }
    }

    startCountDown(): void {
        this.countDown = this.$interval(() => {
            this.secondsLeft--;

            if (this.secondsLeft === 0)
                this.go();

        }, 1000, this.secondsLeft);
    }
}
