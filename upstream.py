"""Real upstream forwarding for xiaoke Gateway.
Handles both streaming and non-streaming OpenAI-compatible API calls."""
from __future__ import annotations
import json
import os
import httpx
import re
from typing import Generator

UPSTREAM_URL = os.environ.get('XIAOKE_UPSTREAM_URL', '').strip().rstrip('/')
UPSTREAM_KEY = os.environ.get('XIAOKE_UPSTREAM_KEY', '').strip()
UPSTREAM_TIMEOUT = int(os.environ.get('XIAOKE_UPSTREAM_TIMEOUT', '120'))

def fix_rp_format(text: str) -> str:
    """修正RP场景下的格式问题：字面\n转真换行，标点之间插入空行"""
    if not text:
        return text
    
    # 规则0：字面 \n 转真换行
    text = text.replace('\\n', '\n')
    
    # 规则1：右括号 后跟 左引号 → 插入空行
    text = re.sub(r'([)）])[\s\u200b]*(["“「])', r'\1\n\n\2', text)
    
    # 规则2：右引号 后跟 左括号 → 插入空行
    text = re.sub(r'(["”」])[\s\u200b]*([(（])', r'\1\n\n\2', text)
    
    # 规则3：右括号 后跟 左括号 → 插入空行
    text = re.sub(r'([)）])[\s\u200b]*([(（])', r'\1\n\n\2', text)
    
    return text



def upstream_config() -> tuple[str, str, int]:
    """Read xiaoke-specific settings first, then the existing gateway config."""
    base_url = (os.environ.get("XIAOKE_UPSTREAM_URL") or os.environ.get("UPSTREAM_BASE_URL") or "").strip().rstrip("/")
    api_key = (os.environ.get("XIAOKE_UPSTREAM_KEY") or os.environ.get("UPSTREAM_API_KEY") or "").strip()
    timeout = int(os.environ.get("XIAOKE_UPSTREAM_TIMEOUT", "120"))
    return base_url, api_key, timeout


def chat_completions_url() -> str:
    base_url, _, _ = upstream_config()
    suffix = "/chat/completions" if base_url.endswith("/v1") else "/v1/chat/completions"
    return f"{base_url}{suffix}"

def _headers() -> dict[str, str]:
    _, api_key, _ = upstream_config()
    h = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    if api_key:
        h['Authorization'] = f'Bearer {api_key}'
    return h


def request_payload(messages: list[dict], model: str, stream: bool, request_options: dict | None = None) -> dict:
    """Rebuild a request after xiaoke has replaced its messages."""
    payload = dict(request_options or {})
    payload.update({'model': model, 'messages': messages, 'stream': stream})
    return payload


def forward_non_stream(messages: list[dict], model: str, request_options: dict | None = None) -> dict:
    """Forward a non-streaming request to upstream and return the full response."""
    url = chat_completions_url()
    payload = request_payload(messages, model, False, request_options)
    with httpx.Client(timeout=upstream_config()[2]) as client:
        resp = client.post(url, json=payload, headers=_headers())
        resp.raise_for_status()
        data = resp.json()
        # 对每个 choice 的 content 做格式修正
        try:
            for choice in data.get('choices', []):
                msg = choice.get('message', {})
                if 'content' in msg and msg['content']:
                    msg['content'] = fix_rp_format(msg['content'])
        except Exception:
            pass
        return data



def forward_stream(messages: list[dict], model: str, request_options: dict | None = None) -> Generator[tuple[str, bool], None, None]:
    """
    假流式：先收集完整响应，格式化后再逐块 yield。
    返回 (sse_event_string, is_done) 元组。
    """
    import time
    
    url = chat_completions_url()
    payload = request_payload(messages, model, True, request_options)
    _, _, timeout_val = upstream_config()
    
    # 1. 收集完整内容
    full_content = ""
    finish_reason = "stop"
    response_id = f"chatcmpl-{int(time.time())}"

    with httpx.Client(timeout=timeout_val) as client:
        with client.stream('POST', url, json=payload, headers=_headers()) as resp:
            resp.raise_for_status()
            buffer = ''
            for chunk in resp.iter_text():
                buffer += chunk
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    if not line:
                        continue
                    print(f"[DEBUG] raw line: {repr(line)}", flush=True)
                    if not line.startswith('data:'):
                        continue
                    data_part = line[5:].strip()
                    print(f"[DEBUG] data_part: {repr(data_part)}", flush=True)
                    if data_part == '[DONE]':
                        continue
                    try:
                        obj = json.loads(data_part)
                        choices = obj.get("choices", [])
                        if choices:
                            delta = choices[0].get("delta", {})
                            c = delta.get("content")
                            print(f"[DEBUG] delta content: {repr(c)}", flush=True)
                            if c:
                                full_content += c
                            fr = choices[0].get("finish_reason")
                            if fr:
                                finish_reason = fr
                    except (json.JSONDecodeError, KeyError, IndexError):
                        continue


    
    # 2. 格式化
    print(f"[DEBUG] BEFORE fix_rp_format: {repr(full_content)}", flush=True)
    full_content = fix_rp_format(full_content) if full_content else ""
    print(f"[DEBUG] AFTER fix_rp_format: {repr(full_content)}", flush=True)

    
    # 3. 假流式输出
    def make_chunk(content_piece: str, finish: str = None) -> str:
        chunk = {
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {"content": content_piece} if content_piece else {},
                "finish_reason": finish
            }]
        }
        return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
    
    if full_content:
        # 按段落切分（双换行）
        parts = full_content.split('\n\n')
        for i, part in enumerate(parts):
            if i > 0:
                yield make_chunk('\n\n', None), False
            if part:
                yield make_chunk(part, None), False
    
    # 最后一个 chunk 带 finish_reason
    yield make_chunk('', finish_reason), False
    
    # [DONE] 信号
    yield "data: [DONE]\n\n", True


def extract_stream_content(sse_events: list[str]) -> str:
    """Extract the full assistant reply from collected SSE events."""
    content_parts = []
    for event_str in sse_events:
        if not event_str.startswith('data:'):
            continue
        data_part = event_str[5:].strip().rstrip('\n')
        if data_part == '[DONE]':
            continue
        try:
            obj = json.loads(data_part)
            choices = obj.get('choices', [])
            if choices:
                delta = choices[0].get('delta', {})
                c = delta.get('content', '')
                if c:
                    content_parts.append(c)
        except (json.JSONDecodeError, IndexError, KeyError):
            continue
    return ''.join(content_parts)
