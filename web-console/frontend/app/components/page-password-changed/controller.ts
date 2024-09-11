

import {StateService} from '@uirouter/angularjs';

export default class implements ng.IPostLink {
    static $inject = ['$state', '$timeout', '$element'];

    constructor($state: StateService, $timeout: ng.ITimeoutService, private el: JQLite) {
        $timeout(() => {
            $state.go('signin');
        }, 10000);
    }

    $postLink() {
        this.el.addClass('public-page');
    }
}
