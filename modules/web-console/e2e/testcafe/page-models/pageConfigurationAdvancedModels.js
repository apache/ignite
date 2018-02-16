import {Selector} from 'testcafe'
import {FormField} from '../components/FormField'
import {isVisible} from '../helpers'

export const createModelButton = Selector('pc-items-table footer-slot .link-success').filter(isVisible)
export const general = {
    generatePOJOClasses: new FormField({id: 'generatePojoInput'}),
    queryMetadata: new FormField({id: 'queryMetadataInput'}),
    keyType: new FormField({id: 'keyTypeInput'}),
    valueType: new FormField({id: 'valueTypeInput'})
}