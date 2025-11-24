# Optional Audio → Text Producer (Stretch)

- Use Whisper small locally to transcribe audio files and publish utterances to `calls.raw`.
- Suggested setup: `openai-whisper` or `faster-whisper` with VAD to segment long audio.
- Output message shape should match `scripts/prepare_dataset.py` output records.


