import {Selector, t} from 'testcafe'

export const pageAdvancedConfiguration = {
    saveButton: Selector('.pc-form-actions-panel .btn-ignite').withText('Save'),
    clusterNavButton: Selector('.pca-menu-link[ui-sref="base.configuration.edit.advanced.cluster"]'),
    modelsNavButton: Selector('.pca-menu-link[ui-sref="base.configuration.edit.advanced.models"]'),
    cachesNavButton: Selector('.pca-menu-link[ui-sref="base.configuration.edit.advanced.caches"]'),
    igfsNavButton: Selector('.pca-menu-link[ui-sref="base.configuration.edit.advanced.igfs"]'),
    async save() {
        await t.click(this.saveButton)
    }
}

export class Panel {
    constructor(title) {
        this._selector = Selector('.pca-panel-heading-title').withText(title).parent('.pca-panel')
        this.heading = this._selector.find('.pca-panel-heading')
        this.body = this._selector.find('.pca-panel-collapse').addCustomDOMProperties({
            isOpened: el => el.classList.contains('in')
        })
    }
}