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