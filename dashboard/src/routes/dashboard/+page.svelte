<script>
    import { onMount } from 'svelte';
    import { listStore, addItem } from '$lib/store';
    import { initializeWebSocket } from '$lib/websocket';

    const liveFeedUrl = import.meta.env.VITE_MARKET_DATA_FEED;

    onMount(() => {
        const cleanup = initializeWebSocket(liveFeedUrl, addItem);
        return cleanup;
    });
</script>

<ul>
    {#each $listStore as item}
        <li>{item}</li>
    {/each}
</ul>
