

import _ from 'lodash';

export default class InputDialogController {
    static $inject = ['deferred', 'ui'];

    constructor(deferred, options) {
        this.deferred = deferred;
        this.options = options;
    }

    confirm() {
        if (_.isFunction(this.options.toValidValue))
            return this.deferred.resolve(this.options.toValidValue(this.options.value));

        this.deferred.resolve(this.options.value);
    }
}
