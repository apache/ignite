

import angular from 'angular';

/**
 * Service to copy some value to OS clipboard.
 * @param {ng.IWindowService} $window
 * @param {ReturnType<typeof import('./Messages.service').default>} Messages
 */
export default function factory($window, Messages) {
    const body = angular.element($window.document.body);

    /** @type {JQuery<HTMLTextAreaElement>} */
    const textArea = angular.element('<textarea/>');

    textArea.css({
        position: 'fixed',
        opacity: '0'
    });

    return {
        /**
         * @param {string} toCopy
         */
        copy(toCopy) {
            textArea.val(toCopy);

            body.append(textArea);

            textArea[0].select();

            try {
                if (document.execCommand('copy'))
                    Messages.showInfo('Value copied to clipboard');
                else
                    window.prompt('Copy to clipboard: Ctrl+C, Enter', toCopy); // eslint-disable-line no-alert
            }
            catch (err) {
                window.prompt('Copy to clipboard: Ctrl+C, Enter', toCopy); // eslint-disable-line no-alert
            }

            textArea.remove();
        }
    };
}

factory.$inject = ['$window', 'IgniteMessages'];
