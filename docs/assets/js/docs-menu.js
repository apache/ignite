const button = document.querySelector('button.menu')
const overlay = document.querySelector('.left-nav__overlay')

const eventTypes = {
    show: 'leftNavigationShow',
    hide: 'leftNavigationHide'
}

/**
 * @param {keyof eventTypes} type
 */
const emit = type => {
    if (!CustomEvent) return
    document.dispatchEvent(new CustomEvent(type, {bubbles: true}))
}

/**
 * @param {boolean} force
 */
const toggleMenu = (force) => {
    const body = document.querySelector('body')
    const HIDE_CLASS = 'hide-left-nav'
    body.classList.toggle(HIDE_CLASS, force)
    emit(eventTypes[body.classList.contains(HIDE_CLASS) ? 'hide' : 'show'])
}

export const hideLeftNav = () => {
    toggleMenu(true, false)
}

if (button && overlay) {
    const query = window.matchMedia('(max-width: 990px)')

    button.addEventListener('click', () => toggleMenu())
    overlay.addEventListener('click', () => toggleMenu())
    query.addListener((e) => {
        toggleMenu(e.matches)
    })
    toggleMenu(query.matches)
}

document.addEventListener('click', e => {
    if (e.target.matches('.left-nav button')) {
        e.target.classList.toggle('expanded')
        e.target.classList.toggle('collapsed')
        e.target.nextElementSibling.classList.toggle('expanded')
        e.target.nextElementSibling.classList.toggle('collapsed')
    }
})
