import {Selector, t} from 'testcafe'
import {FormField} from '../components/FormField'
import {ListEditable} from '../components/ListEditable'

class VersionPicker {
    constructor() {
        this._selector = Selector('version-picker')
    }
    /**
     * @param {string} label Version label
     */
    pickVersion(label) {
        return t
            .hover(this._selector)
            .click(this._selector.find('[role="menuitem"]').withText(label))
    }
}

export class PageConfigurationBasic {
    static SAVE_CHANGES_AND_DOWNLOAD_LABEL = 'Save changes and download project'
    static SAVE_CHANGES_LABEL = 'Save changes'

    constructor() {
        this._selector = Selector('page-configure-basic')
        this.versionPicker = new VersionPicker
        this.totalOffheapSizeInput = Selector('pc-form-field-size#memory')
        this.mainFormAction = Selector('.pc-form-actions-panel .btn-ignite-group .btn-ignite:nth-of-type(1)')
        this.contextFormActionsButton = Selector('.pc-form-actions-panel .btn-ignite-group .btn-ignite:nth-of-type(2)')
        this.contextSaveButton = Selector('a[role=menuitem]').withText(new RegExp(`^${PageConfigurationBasic.SAVE_CHANGES_LABEL}$`))
        this.contextSaveAndDownloadButton = Selector('a[role=menuitem]').withText(PageConfigurationBasic.SAVE_CHANGES_AND_DOWNLOAD_LABEL)
        this.buttonPreviewProject = Selector('button-preview-project')
        this.buttonDownloadProject = Selector('button-download-project')
        this.clusterNameInput = new FormField({id: 'clusterNameInput'})
        this.clusterDiscoveryInput = new FormField({id: 'discoveryInput'})
        this.cachesList = new ListEditable(Selector('.pcb-caches-list'), {
            name: {id: 'nameInput'},
            cacheMode: {id: 'cacheModeInput'},
            atomicityMode: {id: 'atomicityModeInput'},
            backups: {id: 'backupsInput'}
        })
        this.pageHeader = Selector('.pc-page-header')
    }
    async save() {
        await t.click(this.mainFormAction)
    }
    async saveWithoutDownload() {
        return await t.click(this.contextFormActionsButton).click(this.contextSaveButton)
    }
}