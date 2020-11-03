const TAB_BUTTON = document.createRange().createContextualFragment(`
    <button class='code-tabs__tab'></button>
`)

const getAllCodeTabs = () => document.querySelectorAll('code-tabs')

/**
 * @typedef CodeTabsState
 * @prop {string?} currentTab
 * @prop {string[]} tabs
 * @prop {number?} boundingClientRectTop
 */

/**
 * @typedef {number} ScrollState
 */

class CodeTabs {
    /** @param {HTMLElement} el */
    constructor(el) {
        this.el = el
        this.el.codeTabs = this
        /**
         * @type {CodeTabsState}
         */
        this._state = {tabs: []}
    }
    get state() {
        return this._state
    }
    /**
     * @param {CodeTabsState} newState
     */
    set state(newState) {
        const oldState = this._state
        this._state = newState
        this._render(oldState, newState)
    }
    connectedCallback() {
        this._tabElements = this.el.querySelectorAll('code-tab')
        this.state = {
            currentTab: this._tabElements[0].dataset.tab,
            tabs: [...this._tabElements].map(el => el.dataset.tab),
        }
    }
    /**
     * @private
     * @param {CodeTabsState} oldState
     * @param {CodeTabsState} newState
     */
    _render(oldState, newState) {
        if (!oldState.tabs.length && newState.tabs.length) {
            /** @type {HTMLElement} */
            this.el.prepend(newState.tabs.reduce((nav, tab, i) => {
                const button = TAB_BUTTON.firstElementChild.cloneNode()
                button.dataset.tab = tab
                button.innerText = tab
                button.onclick = () => {
                    const scrollState = this._rememberScrollState()
                    this._openTab(tab)
                    this._restoreScrollState(scrollState)
                }
                if (this._tabElements[i].dataset.unavailable) {
                    button.classList.add('grey')      
                }

                this._tabElements[i].button = button
                nav.appendChild(button)
                return nav
            }, document.createElement('NAV')))
            this.el.classList.add('code-tabs__initialized')
        }
        if (oldState.currentTab !== newState.currentTab) {
            for (const tab of this._tabElements) {
                const hidden = tab.dataset.tab !== newState.currentTab
                if (hidden) {
                    tab.setAttribute('hidden', 'hidden')
                } else {
                    tab.removeAttribute('hidden')
                }
                tab.button.classList.toggle('active', !hidden)
            }
        }
    }
    /** 
     * @private
     * @param {string} tab
     */
    _openTab(tab, emitEvent = true) {
        if (!this.state.tabs.includes(tab)) return
        this.state = Object.assign({}, this.state, {currentTab: tab})
        if (emitEvent) this.el.dispatchEvent(new CustomEvent('tabopen', {
            bubbles: true,
            detail: {tab}
        }))
    }
    /** 
     * @param {string} tab
     */
    openTab(tab) {
        this._openTab(tab, false)
    }

    /**
     * @private
     * @returns {ScrollState}
     */
    _rememberScrollState() {
        return this.el.getBoundingClientRect().top
    }

    /**
     * @private
     * @param {ScrollState} scrollState
     * @returns {void}
     */
    _restoreScrollState(scrollState) {
        const currentRectTop = this.el.getBoundingClientRect().top
        const delta = currentRectTop - scrollState
        document.scrollingElement.scrollBy(0, delta)
    }
}

/**
 * @param {NodeListOf<Element>} tabs
 */
const setupSameLanguageSync = (tabs) => {
    document.addEventListener('tabopen', (e) => {
        [...tabs].filter(tab => tab !== e.target).forEach(tab => {
            tab.codeTabs.openTab(e.detail.tab)
        })
    })
}

// Edge does not support custom elements V1
for (const el of getAllCodeTabs()) {
    const instance = new CodeTabs(el)
    instance.connectedCallback()
}
setupSameLanguageSync(getAllCodeTabs())