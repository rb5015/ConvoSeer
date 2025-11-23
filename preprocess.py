#!/usr/bin/env python3
"""
Preprocess transcript files: clean text, remove PII, and chunk into utterances.
Outputs chunks.jsonl with one chunk per line in JSON format.
"""

import json
import re
import os
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict


def count_tokens(text: str) -> int:
    """Approximate token count (words + punctuation)."""
    # Simple approximation: split by whitespace and count
    return len(text.split())


def clean_text(text: str) -> str:
    """
    Clean text: remove punctuation-only lines, normalize whitespace.
    """
    lines = text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        # Remove punctuation-only lines (lines with only punctuation/whitespace)
        stripped = line.strip()
        if not stripped:
            continue
        # Check if line is only punctuation
        if re.match(r'^[\s\W_]+$', stripped):
            continue
        cleaned_lines.append(stripped)
    
    # Join and normalize whitespace
    cleaned = ' '.join(cleaned_lines)
    # Replace multiple spaces with single space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()


def replace_pii(text: str) -> str:
    """
    Replace phone numbers and email addresses with placeholders.
    """
    # Phone number patterns (various formats)
    # Matches: (123) 456-7890, 123-456-7890, 123.456.7890, 1234567890, +1 123-456-7890, etc.
    phone_patterns = [
        r'\+?\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # Standard US format
        r'\(\d{3}\)\s?\d{3}[-.\s]?\d{4}',  # (123) 456-7890
        r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # 123-456-7890
    ]
    
    for pattern in phone_patterns:
        text = re.sub(pattern, '<PHONE>', text)
    
    # Email pattern
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    text = re.sub(email_pattern, '<EMAIL>', text)
    
    return text


def detect_speaker_turns(words: List[Dict], text: str, audio_duration: float = 0.0) -> List[Dict]:
    """
    Detect speaker turns from words array or text.
    Returns list of turns with start_time, end_time, speaker, and text.
    """
    turns = []
    
    # Check if we have speaker information in words
    speakers_available = any(w.get('speaker') is not None for w in words)
    
    if speakers_available and words:
        # Group words by speaker
        current_speaker = None
        current_words = []
        current_start = None
        
        for word in words:
            speaker = word.get('speaker')
            if speaker != current_speaker:
                # Save previous turn
                if current_words and current_start is not None:
                    turn_text = ' '.join([w['text'] for w in current_words])
                    turns.append({
                        'speaker': current_speaker or 'unknown',
                        'start_time': current_start / 1000.0,  # Convert ms to seconds
                        'end_time': current_words[-1]['end'] / 1000.0,
                        'text': turn_text,
                        'words': current_words
                    })
                
                # Start new turn
                current_speaker = speaker
                current_words = [word]
                current_start = word['start']
            else:
                current_words.append(word)
        
        # Add last turn
        if current_words and current_start is not None:
            turn_text = ' '.join([w['text'] for w in current_words])
            turns.append({
                'speaker': current_speaker or 'unknown',
                'start_time': current_start / 1000.0,
                'end_time': current_words[-1]['end'] / 1000.0,
                'text': turn_text,
                'words': current_words
            })
    else:
        # No speaker info available - use heuristics to detect turns
        # Look for sentence boundaries and pauses in timestamps
        if words:
            # Group words by pauses (gaps > 1 second indicate speaker change)
            PAUSE_THRESHOLD_MS = 1000
            
            current_words = []
            current_start = words[0]['start'] if words else None
            prev_end = None
            
            for word in words:
                if prev_end is not None and (word['start'] - prev_end) > PAUSE_THRESHOLD_MS:
                    # Significant pause - likely speaker change
                    if current_words:
                        turn_text = ' '.join([w['text'] for w in current_words])
                        turns.append({
                            'speaker': 'unknown',
                            'start_time': current_start / 1000.0,
                            'end_time': current_words[-1]['end'] / 1000.0,
                            'text': turn_text,
                            'words': current_words
                        })
                    current_words = [word]
                    current_start = word['start']
                else:
                    if not current_words:
                        current_start = word['start']
                    current_words.append(word)
                
                prev_end = word['end']
            
            # Add last turn
            if current_words and current_start is not None:
                turn_text = ' '.join([w['text'] for w in current_words])
                turns.append({
                    'speaker': 'unknown',
                    'start_time': current_start / 1000.0,
                    'end_time': current_words[-1]['end'] / 1000.0,
                    'text': turn_text,
                    'words': current_words
                })
        else:
            # Fallback: split text by sentences
            sentences = re.split(r'[.!?]+\s+', text)
            # Approximate timestamps (distribute evenly)
            if sentences:
                time_per_sentence = audio_duration / len(sentences) if audio_duration > 0 else 0
                for i, sentence in enumerate(sentences):
                    if sentence.strip():
                        turns.append({
                            'speaker': 'unknown',
                            'start_time': i * time_per_sentence,
                            'end_time': (i + 1) * time_per_sentence,
                            'text': sentence.strip(),
                            'words': []
                        })
    
    return turns


def split_long_turn(turn: Dict, max_tokens: int = 200, overlap_tokens: int = 50) -> List[Dict]:
    """
    Split a long turn into smaller chunks with overlap.
    """
    text = turn['text']
    tokens = text.split()
    num_tokens = len(tokens)
    
    if num_tokens <= max_tokens:
        return [turn]
    
    chunks = []
    start_idx = 0
    chunk_idx = 0
    
    # Calculate time per token for timestamp estimation
    duration = turn['end_time'] - turn['start_time']
    time_per_token = duration / num_tokens if num_tokens > 0 else 0
    
    while start_idx < num_tokens:
        end_idx = min(start_idx + max_tokens, num_tokens)
        chunk_tokens = tokens[start_idx:end_idx]
        chunk_text = ' '.join(chunk_tokens)
        
        # Estimate timestamps
        chunk_start_time = turn['start_time'] + (start_idx * time_per_token)
        chunk_end_time = turn['start_time'] + (end_idx * time_per_token)
        
        chunk = {
            'speaker': turn['speaker'],
            'start_time': chunk_start_time,
            'end_time': chunk_end_time,
            'text': chunk_text,
            'original_turn_index': turn.get('turn_index', 0),
            'chunk_sequence': chunk_idx,
            'words': turn.get('words', [])[start_idx:end_idx] if turn.get('words') else []
        }
        chunks.append(chunk)
        
        # Move start forward with overlap
        start_idx = end_idx - overlap_tokens
        chunk_idx += 1
        
        # Prevent infinite loop
        if start_idx >= num_tokens - overlap_tokens:
            break
    
    return chunks


def process_transcript(file_path: Path, transcript_id: str) -> List[Dict]:
    """
    Process a single transcript file and return chunks.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract text and metadata
    raw_text = data.get('text', '')
    words = data.get('words', [])
    audio_duration = data.get('audio_duration', 0)
    
    # Clean text
    cleaned_text = clean_text(raw_text)
    
    # Replace PII
    cleaned_text = replace_pii(cleaned_text)
    
    # Detect speaker turns
    turns = detect_speaker_turns(words, cleaned_text, audio_duration)
    
    # If no turns detected, create a single turn from cleaned text
    if not turns:
        turns = [{
            'speaker': 'unknown',
            'start_time': 0.0,
            'end_time': audio_duration,
            'text': cleaned_text,
            'words': []
        }]
    
    # Add turn index
    for i, turn in enumerate(turns):
        turn['turn_index'] = i
        # Clean and replace PII in turn text
        turn['text'] = replace_pii(clean_text(turn['text']))
    
    # Split long turns and create chunks
    all_chunks = []
    chunk_id = 0
    
    for turn in turns:
        num_tokens = count_tokens(turn['text'])
        
        if num_tokens > 300:
            # Split into smaller chunks
            sub_chunks = split_long_turn(turn, max_tokens=200, overlap_tokens=50)
            for sub_chunk in sub_chunks:
                chunk = {
                    'transcript_id': transcript_id,
                    'chunk_id': f"{transcript_id}_chunk_{chunk_id}",
                    'speaker': sub_chunk['speaker'],
                    'start_time': sub_chunk['start_time'],
                    'end_time': sub_chunk['end_time'],
                    'original_turn_text': turn['text'],
                    'chunk_text': sub_chunk['text'],
                    'turn_index': sub_chunk.get('original_turn_index', turn['turn_index']),
                    'chunk_sequence': sub_chunk.get('chunk_sequence', 0),
                    'num_tokens': count_tokens(sub_chunk['text'])
                }
                all_chunks.append(chunk)
                chunk_id += 1
        else:
            # Use turn as-is
            chunk = {
                'transcript_id': transcript_id,
                'chunk_id': f"{transcript_id}_chunk_{chunk_id}",
                'speaker': turn['speaker'],
                'start_time': turn['start_time'],
                'end_time': turn['end_time'],
                'original_turn_text': turn['text'],
                'chunk_text': turn['text'],
                'turn_index': turn['turn_index'],
                'chunk_sequence': 0,
                'num_tokens': num_tokens
            }
            all_chunks.append(chunk)
            chunk_id += 1
    
    return all_chunks


def find_transcript_files(input_dir: Path) -> List[Path]:
    """Find all JSON transcript files in the input directory."""
    transcript_files = []
    
    # Search recursively for JSON files
    for json_file in input_dir.rglob('*.json'):
        # Skip if it's not a transcript file (could add more filters)
        transcript_files.append(json_file)
    
    return sorted(transcript_files)


def main():
    parser = argparse.ArgumentParser(
        description='Preprocess transcript files: clean text, remove PII, and chunk into utterances.'
    )
    parser.add_argument(
        '--input-dir',
        type=str,
        default='datasets',
        help='Directory containing transcript JSON files (default: datasets)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='chunks.jsonl',
        help='Output JSONL file path (default: chunks.jsonl)'
    )
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_file = Path(args.output)
    
    if not input_dir.exists():
        print(f"Error: Input directory '{input_dir}' does not exist.")
        return 1
    
    # Find all transcript files
    transcript_files = find_transcript_files(input_dir)
    
    if not transcript_files:
        print(f"No JSON transcript files found in '{input_dir}'")
        return 1
    
    print(f"Found {len(transcript_files)} transcript file(s)")
    
    # Process all transcripts
    all_chunks = []
    
    for file_path in transcript_files:
        # Use filename (without extension) as transcript_id
        transcript_id = file_path.stem
        
        try:
            print(f"Processing: {file_path.name}")
            chunks = process_transcript(file_path, transcript_id)
            all_chunks.extend(chunks)
            print(f"  -> Generated {len(chunks)} chunk(s)")
        except Exception as e:
            print(f"  -> Error processing {file_path.name}: {e}")
            continue
    
    # Write chunks to JSONL file
    print(f"\nWriting {len(all_chunks)} chunk(s) to {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        for chunk in all_chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + '\n')
    
    print(f"Done! Output written to {output_file}")
    return 0


if __name__ == '__main__':
    exit(main())

