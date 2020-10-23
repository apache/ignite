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

const rightNav = document.querySelector('.right-nav .sectlevel1')

if (IntersectionObserver && rightNav) {
    const tocAnchors = [...rightNav.querySelectorAll('a[href]')]
    let last;
    tocAnchors.forEach((a, i, all) => {
        const target = document.querySelector(`${a.hash}`)
        if (!target) return
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                a.classList.toggle('visible', entry.isIntersecting)
                if (entry.isIntersecting) last = a

                const firstVisible = rightNav.querySelector('.visible')
                tocAnchors.forEach(a => a.classList.remove('active'))
                if (firstVisible) firstVisible.classList.add('active')
                else if (last) last.classList.add('active')
            })
        });
        observer.observe(target)
    })
}
