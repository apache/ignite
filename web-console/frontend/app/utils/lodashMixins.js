

import negate from 'lodash/negate';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';

export const nonNil = negate(isNil);
export const nonEmpty = negate(isEmpty);
