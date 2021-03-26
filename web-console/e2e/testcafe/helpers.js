/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { ClientFunction } = require('testcafe');

const mouseenterTrigger = ClientFunction((selector = '') => {
    return new Promise((resolve) => {
        window.jQuery(selector).mouseenter();

        resolve();
    });
});

const getLocationPathname = ClientFunction(() => Promise.resolve(location.pathname));

/**
 * Fake visibility predicate, use with selector.filter method.
 *
 * @param {Element} node
 */
const isVisible = (node) => !!node.getBoundingClientRect().width;

const scrollIntoView = ClientFunction(() => {
    el().scrollIntoView();
    document.scrollingElement.scrollTop += document.querySelector('.wrapper > .content').offsetTop;
});

const scrollToPageBottom = ClientFunction(() => document.scrollingElement.scrollTop = 9999999);
const scrollBy = ClientFunction(() => document.scrollingElement.scrollTop += amount);

module.exports = { mouseenterTrigger, getLocationPathname, isVisible, scrollIntoView, scrollToPageBottom, scrollBy };
