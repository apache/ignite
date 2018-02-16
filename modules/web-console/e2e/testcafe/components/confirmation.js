import {Selector, t} from 'testcafe'

export const confirmation = {
    body: Selector('.modal-body'),
    async confirm() {
        await t.click('#confirm-btn-ok')
    },
    async cancel() {
        await t.click('#confirm-btn-cancel')
    },
    async close() {
        await t.click('.modal .close')
    }
}