

import {CancellationError} from 'app/errors/CancellationError';

/**
 * Service to show various information and error messages.
 * @param {mgcrea.ngStrap.alert.IAlertService} $alert
 * @param {import('./ErrorParser.service').default} errorParser
 */
export default function factory($alert, errorParser) {
    // Common instance of alert modal.
    let msgModal;

    const hideAlert = () => {
        if (msgModal) {
            msgModal.hide();
            msgModal.destroy();
            msgModal = null;
        }
    };

    const _showMessage = (message, err, type, duration) => {
        hideAlert();

        const parsedErr = err ? errorParser.parse(err, message) : errorParser.parse(message, null);
        const causes = parsedErr.causes;
        const title = parsedErr.message + (causes.length ? '<ul><li>' + causes.join('</li><li>') + '</li></ul>See node logs for more details.' : '');

        msgModal = $alert({type, title, duration: duration + causes.length * 5});

        msgModal.$scope.icon = `icon-${type}`;
    };

    return {
        hideAlert,
        /**
         * @param {string|CancellationError} message
         * @param [err]
         * @param duration Popover visibility duration in seconds.
         */
        showError(message, err, duration = 10) {
            if (message instanceof CancellationError)
                return false;

            _showMessage(message, err, 'danger', duration);

            return false;
        },
        /**
         * @param {string} message
         * @param duration Popover visibility duration in seconds.
         */
        showInfo(message, duration = 5) {
            _showMessage(message, null, 'success', duration);
        }
    };
}

factory.$inject = ['$alert', 'IgniteErrorParser'];
