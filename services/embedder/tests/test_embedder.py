import importlib.util
import pathlib
import math
import torch


def load_embedder_main():
    base = pathlib.Path(__file__).resolve().parents[1]  # services/embedder
    spec = importlib.util.spec_from_file_location("embedder_main", base / "main.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_dummy_tokenizer(texts, padding=True, truncation=True, max_length=None, return_tensors="pt"):
    # Return simple tensors for input_ids and attention_mask
    max_len = max(len(t.split()) for t in texts)
    batch = len(texts)
    input_ids = torch.ones((batch, max_len), dtype=torch.long)
    attention_mask = torch.ones((batch, max_len), dtype=torch.long)
    return {"input_ids": input_ids, "attention_mask": attention_mask}


class DummyOutputs:
    def __init__(self, last_hidden_state):
        self.last_hidden_state = last_hidden_state


class DummyModel:
    def __init__(self, device="cpu", hidden_size=16):
        self.device = torch.device(device)
        self.hidden_size = hidden_size
        # Add a dummy parameter so .parameters() works
        self._param = torch.nn.Parameter(torch.randn(1, dtype=torch.float32))

    def parameters(self):
        """Return model parameters for dtype detection."""
        return [self._param]

    def __call__(self, **batch_dict):
        input_ids = batch_dict.get("input_ids")
        batch_size, seq_len = input_ids.shape
        # create a deterministic last_hidden_state
        lh = torch.arange(batch_size * seq_len * self.hidden_size, dtype=torch.float32, device=self.device)
        lh = lh.reshape(batch_size, seq_len, self.hidden_size)
        return DummyOutputs(lh)


def test_embed_batch_monkeypatch():
    mod = load_embedder_main()

    texts = ["hello world", "foo bar baz"]

    # provide a fake _get_model_and_tokenizer that returns our dummy tokenizer and model
    def fake_get(name):
        return (lambda *a, **k: make_dummy_tokenizer(texts, **k), DummyModel(device="cpu", hidden_size=16))

    setattr(mod, "_get_model_and_tokenizer", fake_get)

    vectors = mod._embed_batch(texts, "any-model")

    assert isinstance(vectors, list)
    assert len(vectors) == len(texts)
    # each vector length equals hidden size
    assert len(vectors[0]) == 16
    # embeddings should be L2-normalized -> norm approx 1
    norm0 = math.sqrt(sum([x * x for x in vectors[0]]))
    assert abs(norm0 - 1.0) < 1e-3


def test_embed_endpoint_monkeypatch():
    mod = load_embedder_main()

    texts = ["single text"]

    def fake_get(name):
        return (lambda *a, **k: make_dummy_tokenizer(texts, **k), DummyModel(device="cpu", hidden_size=8))

    setattr(mod, "_get_model_and_tokenizer", fake_get)

    req = mod.EmbedRequest(texts=texts, model=None)
    resp = mod.embed(req)

    assert resp.model == mod.DEFAULT_MODEL
    assert isinstance(resp.embeddings, list)
    assert len(resp.embeddings) == 1
    assert len(resp.embeddings[0]) == 8
