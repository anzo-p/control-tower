import type { ListItem } from '$lib/store';

export function initializeWebSocket(url: string, onNewData: (data: ListItem) => void) {
    const ws = new WebSocket(url);

    ws.onmessage = (event: MessageEvent) => {
        const newElement: ListItem = event.data;
        onNewData(newElement);
    };

    ws.onerror = (error: Event) => {
        console.error('WebSocket Error:', error);
    };

    ws.onclose = () => {
        console.log('WebSocket connection closed');
    };

    return () => {
        ws.close();
    };
}
