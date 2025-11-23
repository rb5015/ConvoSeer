# Performance Notes

- Embedding batching: `/embed` endpoint supports list inputs; the consumer batches up to `EMBED_BATCH` (default 64).
- Caching: LRU caches added in embedder and RAG services to avoid re-embedding duplicates.
- Vector Search: Start with k=8 and numCandidates ~k*20; adjust based on recall vs latency.
- Filtering: Apply `metadata.industry/product/sentiment` filters to shrink candidate set before vector stage.


