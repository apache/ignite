import {Selector, t} from 'testcafe'

export class FormField {
    static ROOT_SELECTOR = '.ignite-form-field'
    static LABEL_SELECTOR = '.ignite-form-field__label'
    static CONTROL_SELECTOR = '[ng-model]'
    static ERRORS_SELECTOR = '.ignite-form-field__errors'

    constructor({id = '', label = ''} = {}) {
        if (!id && !label) throw new Error('ID or label are required')
        if (id) {
            this._selector = Selector(`#${id}`).parent(FormField.ROOT_SELECTOR)
        } else if (label) {
            this._selector = Selector(() => {
                return Array
                    .from(window.document.querySelectorAll(LABEL_CLASS))
                    .filter(el => el.textContent.contains(label))
                    .map(el => el.parent(FormField.ROOT_SELECTOR))
                    .pop()
            })
        }
        this.label = this._selector.find(FormField.LABEL_SELECTOR)
        this.control = this._selector.find(FormField.CONTROL_SELECTOR)
        this.errors = this._selector.find(FormField.ERRORS_SELECTOR)
    }
    /**
     * Selects dropdown option
     * @param {string} label
     */
    async selectOption(label) {
        await t
            .click(this.control)
            .click(Selector('.bssm-item-button').withText(label))
    }
}