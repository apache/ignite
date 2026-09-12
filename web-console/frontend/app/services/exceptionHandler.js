

import {CancellationError} from 'app/errors/CancellationError';

/**
 * @param {ng.ILogService} $log
 */
export function $exceptionHandler($log) {
    return function(exception, cause) {
        if (exception instanceof CancellationError)
            return;

        // From ui-grid
        if (exception === 'Possibly unhandled rejection: canceled')
            return;

        $log.error(exception, cause);
    };
}

$exceptionHandler.$inject = ['$log'];
