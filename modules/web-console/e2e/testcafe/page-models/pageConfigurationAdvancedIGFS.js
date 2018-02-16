import {Selector} from 'testcafe'
import {isVisible} from '../helpers'

export const createIGFSButton = Selector('pc-items-table footer-slot .link-success').filter(isVisible)