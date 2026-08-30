# -*- coding: utf-8 -*-
"""DSH Bot Bridge HTTP 客户端（AstrBot 侧）。

对应 dsh-bot-bridge 插件暴露的 /api/bot/* 端点：
  - prompt_stream(): POST /api/bot/prompt, 逐帧解析 SSE 返回事件 dict
  - answer_question() / approve() / cancel(): 提问、授权、停止
  - history() / get_session() / reset() / health(): 历史、会话绑定、健康检查

本模块只依赖 aiohttp, 与 dsh-bot-bridge 之间只有 HTTP 契约, 无其他耦合。
"""

import json
import logging
from typing import Any, AsyncIterator, Dict, List, Optional

import aiohttp

logger = logging.getLogger("astrbot")


class DshBridgeError(Exception):
    """桥接请求失败。"""

    def __init__(self, message: str, code: str = ""):
        super().__init__(message)
        self.code = code


class DshBridgeClient:
    """dsh-bot-bridge 的极简 HTTP 客户端。"""

    def __init__(self, base_url: str, token: str, timeout: float = 30.0):
        self.base_url = (base_url or "").strip().rstrip("/")
        self.token = (token or "").strip()
        self.timeout = float(timeout or 30.0)
        if not self.base_url:
            raise DshBridgeError("dsh_base_url 未配置", "missing-base-url")
        if not self.token:
            raise DshBridgeError("dsh_bot_token 未配置", "missing-token")

    def _headers(self) -> Dict[str, str]:
        return {"x-bot-token": self.token}

    async def health(self) -> Dict[str, Any]:
        """GET /api/bot/health"""
        return await self._request_json("GET", "/api/bot/health")

    async def get_session(self, client_id: str) -> Optional[str]:
        """GET /api/bot/session?clientId=... → sessionId 或 None"""
        data = await self._request_json("GET", f"/api/bot/session?clientId={client_id}")
        return data.get("sessionId") or None

    async def reset(self, client_id: str) -> Dict[str, Any]:
        """POST /api/bot/reset 解绑会话。"""
        return await self._request_json("POST", "/api/bot/reset", {"clientId": client_id})

    async def history(self, session_id: str, limit: int = 20) -> Dict[str, Any]:
        """GET /api/bot/history?sessionId=...&limit=..."""
        return await self._request_json(
            "GET", f"/api/bot/history?sessionId={session_id}&limit={int(limit)}"
        )

    async def answer_question(
        self, session_id: str, rpc_id: str, answers: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """POST /api/bot/answer 回答 ask_user_question。"""
        return await self._request_json(
            "POST",
            "/api/bot/answer",
            {"sessionId": session_id, "rpcId": rpc_id, "answers": answers},
        )

    async def approve(
        self, session_id: str, rpc_id: str, approval_id: str, outcome: str
    ) -> Dict[str, Any]:
        """POST /api/bot/approve 批准/拒绝工具授权。"""
        return await self._request_json(
            "POST",
            "/api/bot/approve",
            {
                "sessionId": session_id,
                "rpcId": rpc_id,
                "approvalId": approval_id,
                "outcome": outcome,
            },
        )

    async def cancel(self, session_id: str) -> Dict[str, Any]:
        """POST /api/bot/cancel 打断当前 turn。"""
        return await self._request_json(
            "POST", "/api/bot/cancel", {"sessionId": session_id}
        )

    async def prompt_stream(
        self,
        client_id: str,
        text: str,
        session_id: Optional[str] = None,
        reset: bool = False,
    ) -> AsyncIterator[Dict[str, Any]]:
        """POST /api/bot/prompt, 逐帧产出 SSE 事件 dict。

        事件 type: status / text / tool / question / approval / error / done / ping。
        """
        payload: Dict[str, Any] = {"clientId": client_id, "text": text, "reset": reset}
        if session_id:
            payload["sessionId"] = session_id

        timeout = aiohttp.ClientTimeout(total=None, connect=self.timeout, sock_read=300.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(
                    f"{self.base_url}/api/bot/prompt",
                    json=payload,
                    headers=self._headers(),
                ) as resp:
                    if resp.status != 200:
                        body = (await resp.text(errors="replace"))[:300]
                        raise DshBridgeError(
                            f"DSH bridge HTTP {resp.status}: {body}", "http-error"
                        )
                    buffer = ""
                    async for chunk in resp.content:
                        buffer += chunk.decode("utf-8", "replace")
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            event = self._parse_sse_line(line)
                            if event is not None:
                                yield event
                # 剩余半行（一般没有）
                event = self._parse_sse_line(buffer)
                if event is not None:
                    yield event
            except aiohttp.ClientError as e:
                raise DshBridgeError(f"无法连接 DSH bridge: {e}", "connect-error") from e

    @staticmethod
    def _parse_sse_line(line: str) -> Optional[Dict[str, Any]]:
        """解析一行 SSE: data: {...}"""
        line = line.strip()
        if not line or not line.startswith("data:"):
            return None
        raw = line[5:].strip()
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except ValueError:
            logger.warning(f"dsh bridge SSE 帧解析失败: {raw[:120]!r}")
            return None
        return data if isinstance(data, dict) else None

    async def _request_json(
        self, method: str, path: str, payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                kwargs: Dict[str, Any] = {"headers": self._headers()}
                if payload is not None:
                    kwargs["json"] = payload
                async with session.request(method, f"{self.base_url}{path}", **kwargs) as resp:
                    text = await resp.text(errors="replace")
                    if resp.status != 200:
                        raise DshBridgeError(
                            f"DSH bridge HTTP {resp.status}: {text[:300]}", "http-error"
                        )
                    try:
                        data = json.loads(text)
                    except ValueError as e:
                        raise DshBridgeError("DSH bridge 返回非 JSON", "bad-json") from e
                    if not isinstance(data, dict):
                        raise DshBridgeError("DSH bridge 返回格式错误", "bad-json")
                    if data.get("ok") is not True:
                        err = data.get("error") or {}
                        raise DshBridgeError(
                            str(err.get("message") or err.get("code") or "unknown error"),
                            str(err.get("code") or ""),
                        )
                    return data
            except aiohttp.ClientError as e:
                raise DshBridgeError(f"无法连接 DSH bridge: {e}", "connect-error") from e
