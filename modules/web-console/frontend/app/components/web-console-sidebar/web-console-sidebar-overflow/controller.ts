/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import ResizeObserver from 'resize-observer-polyfill';

export default class WebCOnsoleSidebarOverflow {
    static $inject = ['$element', 'gridUtil', '$window'];

    constructor(private el: JQLite, private gridUtil: {getScrollbarWidth(): number}, private $win: ng.IWindowService) {}

    scrollEl!: JQLite;

    resizeObserver: ResizeObserver;

    $onInit() {
        this.el.css('--scrollbar-width', this.gridUtil.getScrollbarWidth());
    }

    $postLink() {
        this.scrollEl[0].addEventListener('scroll', this.onScroll, {passive: true});
        this.resizeObserver = new ResizeObserver(() => this.applyStyles(this.scrollEl[0]));
        this.resizeObserver.observe(this.el[0]);
    }
    $onDestroy() {
        this.scrollEl[0].removeEventListener('scroll', this.onScroll);
        this.resizeObserver.disconnect();
    }
    applyStyles(target: HTMLElement) {
        const {offsetHeight, scrollTop, scrollHeight} = target;
        const top = scrollTop !== 0;
        const bottom = Math.floor((offsetHeight + scrollTop)) !== Math.floor(scrollHeight);

        target.classList.toggle('top', top);
        target.classList.toggle('bottom', bottom);
    }
    onScroll = (e: UIEvent) => {
        this.$win.requestAnimationFrame(() => {
            this.applyStyles(e.target as HTMLElement);
        });
    }
}
