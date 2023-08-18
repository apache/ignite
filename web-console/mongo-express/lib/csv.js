import { Parser } from '@json2csv/plainjs';
import flat from 'flat';
import { isPlainObject } from 'lodash-es';

const handleObject = function (data) {
  for (const x in data) {
    if (data[x] && data[x].constructor.name === 'ObjectId') {
      data[x] = ['ObjectId("', data[x], '")'].join('');
    } else if (isPlainObject(data[x])) {
      handleObject(data[x]);
    }
  }
};

export default function (data) {
  for (let i = 0, ii = data.length; i < ii; i++) {
    const current = data[i];
    handleObject(current);
    data[i] = flat(current, { safe: true });
  }
  const parser = new Parser({ eol: '\n' });
  return parser.parse(data);
}
