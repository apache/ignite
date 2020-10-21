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
