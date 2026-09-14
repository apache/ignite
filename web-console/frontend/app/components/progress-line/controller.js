

const INDETERMINATE_CLASS = 'progress-line__indeterminate';
const COMPLETE_CLASS = 'progress-line__complete';

/**
 * @typedef {-1} IndeterminateValue
 */

/**
 * @typedef {1} CompleteValue
 */

/**
 * @typedef {IndeterminateValue|CompleteValue} ProgressLineValue
 */

export default class ProgressLine {
    /** @type {ProgressLineValue} */
    value;

    static $inject = ['$element'];

    /**
     * @param {JQLite} $element
     */
    constructor($element) {
        this.$element = $element;
    }

    /**
     * @param {{value: ng.IChangesObject<ProgressLineValue>}} changes
     */
    $onChanges(changes) {
        if (changes.value.currentValue === -1) {
            this.$element[0].classList.remove(COMPLETE_CLASS);
            this.$element[0].classList.add(INDETERMINATE_CLASS);
            return;
        }
        if (typeof changes.value.currentValue === 'number') {
            if (changes.value.currentValue === 1) this.$element[0].classList.add(COMPLETE_CLASS);
            this.$element[0].classList.remove(INDETERMINATE_CLASS);
        }
    }
}
