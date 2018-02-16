import {Selector, t} from 'testcafe'

export class PageConfigurationAdvancedCluster {
    constructor() {
        this._selector = Selector('page-configure-advanced-cluster')
        this.saveButton = Selector('.pc-form-actions-panel .btn-ignite').withText('Save')
    }
    async save() {
        await t.click(this.saveButton)
    }
}