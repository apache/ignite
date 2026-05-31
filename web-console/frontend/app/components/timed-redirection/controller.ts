

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
