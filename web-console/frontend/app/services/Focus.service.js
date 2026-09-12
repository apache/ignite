

/**
 * Service to transfer focus for specified element.
 * @param {ng.ITimeoutService} $timeout
 */
export default function factory($timeout) {
    return {
        /**
         * @param {string} id Element id
         */
        move(id) {
            // Timeout makes sure that is invoked after any other event has been triggered.
            // E.g. click events that need to run before the focus or inputs elements that are
            // in a disabled state but are enabled when those events are triggered.
            $timeout(() => {
                const elem = $('#' + id);

                if (elem.length > 0)
                    elem[0].focus();
            }, 100);
        }
    };
}

factory.$inject = ['$timeout'];
