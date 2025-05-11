

export const uniqueName = (name, items, fn = ({name, i}) => `${name}${i}`) => {
    let i = 0;
    let newName = name;
    const isUnique = (item) => item.name === newName;
    while (items.some(isUnique)) {
        i += 1;
        newName = fn({name, i});
    }
    return newName;
};
