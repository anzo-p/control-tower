import { writable } from 'svelte/store';

export type ListItem = string;

const list = writable<ListItem[]>([]);
const maxElements = 10;

export function addItem(item: ListItem) {
    console.log('Adding item', item);
    list.update((items) => {
        let newItems = [item, ...items];
        if (newItems.length > maxElements) {
            newItems = newItems.slice(0, maxElements);
        }
        return newItems;
    });
}

export const listStore = {
    subscribe: list.subscribe,
    addItem
};
