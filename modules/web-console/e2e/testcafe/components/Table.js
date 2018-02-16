import {Selector, t} from 'testcafe'

const findCell = Selector((table, rowIndex, columnLabel) => {
    table = table()
    const columnIndex = [].constructor.from(
        table.querySelectorAll('.ui-grid-header-cell:not(.ui-grid-header-span)'),
        e => e.textContent
    ).findIndex(t => t.includes(columnLabel))
    const row = table.querySelector(`.ui-grid-render-container:not(.left) .ui-grid-viewport .ui-grid-row:nth-of-type(${rowIndex+1})`)
    const cell = row.querySelector(`.ui-grid-cell:nth-of-type(${columnIndex})`)
    return cell
})

export class Table {
    constructor(selector) {
        this._selector = selector
        this.title = this._selector.find('.panel-title')
        this.actionsButton = this._selector.find('.btn-ignite').withText('Actions')
    }
    async performAction(label) {
        await t.hover(this.actionsButton).click(Selector('.dropdown-menu a').withText(label))
    }
    /**
     * Toggles grid row selection
     * @param {number} index Index of row, starting with 1
     */
    async toggleRowSelection(index) {
        await t.click(this._selector.find(`.ui-grid-pinned-container .ui-grid-row:nth-of-type(${index}) .ui-grid-selection-row-header-buttons`))
    }
    findCell(rowIndex, columnLabel) {
        return Selector(findCell(this._selector, rowIndex, columnLabel))
    }
}