// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

const query = window.matchMedia('(max-width: 450px)')
const header = document.querySelector('header')
const search = document.querySelector('header .search input')

let state = {
    narrowMode: false,
    showSearch: false,
    showNav: false
}

const eventTypes = {
    navShow: 'topNavigationShow',
    navHide: 'topNavigationHide'
}

/**
 * @param {keyof eventTypes} type
 */
const emit = type => {
    if (!CustomEvent) return
    header.dispatchEvent(new CustomEvent(type, {bubbles: true}))
}

/**
 * @param {typeof state} newState
 */
const render = (newState) => {
    if (state.narrowMode !== newState.narrowMode)
        header.classList.toggle('narrow-header')
    if (state.showSearch !== newState.showSearch) {
        header.classList.toggle('show-search')
        search.value = ''
        if (newState.showSearch) search.focus()
    }
    if (state.showNav !== newState.showNav) {
        header.classList.toggle('show-nav')
        emit(eventTypes[newState.showNav ? 'navShow' : 'navHide'])        
    }
    state = newState
}

render(Object.assign({}, state, {narrowMode: query.matches}))

query.addListener((e) => {
    render(Object.assign({}, state, {
        narrowMode: e.matches,
        showSearch: false,
        showNav: false
    }))
})

document.querySelector('.top-nav-toggle').addEventListener('click', () => {
    render(Object.assign({}, state, {
        showNav: !state.showNav,
        showSearch: false
    }))
})

document.querySelector('.search-toggle').addEventListener('click', () => {
    render(Object.assign({}, state, {
        showSearch: !state.showSearch,
        showNav: false
    }))
})

search.addEventListener('blur', () => {
    render(Object.assign({}, state, {
        showSearch: false
    }))
})

export const hideTopNav = () => {
    render(Object.assign({}, state, {
        showNav: false,
        showSearch: false
    }))
}
