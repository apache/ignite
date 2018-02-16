import {Selector, t} from 'testcafe'
import {Table} from '../components/Table'

export class PageConfigurationOverview {
    constructor() {
        this.createClusterConfigButton = Selector('.btn-ignite').withText('Create Cluster Configuration')
        this.importFromDBButton = Selector('.btn-ignite').withText('Import from Database')
        this.clustersTable = new Table(Selector('pc-items-table'))
        this.pageHeader = Selector('.pc-page-header')
    }
}