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

const BUTTON_CLASSNAME = 'copy-to-clipboard-button'
const BUTTON_CLASSNAME_SUCCESS = 'copy-to-clipboard-button__success'

const TEMPLATE = document.createElement('button')
TEMPLATE.classList.add(BUTTON_CLASSNAME)
TEMPLATE.title = 'Copy to clipboard'
TEMPLATE.type = 'button'

const SECOND = 1000
const RESULT_DISPLAY_DURATION = 0.5 * SECOND

/**
 * @param {HTMLElement?} el Element to copy text from
 * @returns {boolean} Copy success/failure
 */
function copyCode(el) {
	if (!el) return
	if (!el.matches('code')) return

	const range = document.createRange()
	range.selectNode(el)
	window.getSelection().addRange(range)

	try {
		return document.execCommand('copy')
	} catch (err) {} finally {
		window.getSelection().removeAllRanges()		
	}
}

function init() {
	for (const code of document.querySelectorAll('pre>code')) {
		try {
			const container = code.closest('.listingblock .content')
			if (!container) break
			const button = TEMPLATE.cloneNode(true)
			container.appendChild(button)			
		} catch (err) {}
	}
	document.addEventListener('click', e => {
		if (e.target.classList.contains(BUTTON_CLASSNAME)) {
			const result = copyCode(e.target.parentElement.querySelector('code'))
			if (result) {
				e.target.innerText = '✓'
				e.target.classList.add(BUTTON_CLASSNAME_SUCCESS)
				setTimeout(() => {
					e.target.innerText = TEMPLATE.textContent
					e.target.classList.remove(BUTTON_CLASSNAME_SUCCESS)
				}, RESULT_DISPLAY_DURATION)
			}
		}
	})
}

window.addEventListener('load', init)
