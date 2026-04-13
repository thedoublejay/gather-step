import { helper } from './utils';
import defaultExport from './lib';

export function processItem(item) {
    const result = helper(item);
    defaultExport(result);
    return result;
}

export const transform = (x) => helper(x);
