# -*- coding: utf-8 -*-
"""DSH 远程控制插件（AstrBot）。

指定白名单用户可在私聊中通过本插件接入 DeepSeek Harness (DSH)：
  - 把用户消息发往 dsh-bot-bridge (DSH 宿主插件) 的 /api/bot/prompt；
  - 实时流式转发 harness 的"非思考"输出（模型正文、工具动态）到 QQ；
  - 支持多轮会话（clientId 绑定）、提问/授权交互应答、历史查看、停止。

命令（需 @ 或唤醒词触发，仅白名单用户 + 默认仅私聊）：
  dsh <内容>        把内容交给 DSH 执行（首次自动创建会话）
  dsh 新会话/reset   解绑并新建会话
  dsh 状态/status    查看会话与执行状态
  dsh 历史/history   查看最近输出
  dsh 停止/stop      打断当前任务
  dsh 帮助/help      使用说明

与 DSH 的耦合面只有 dsh-bot-bridge 的 HTTP 契约：bridge 不在时本插件仍
可正常加载，仅在执行 dsh 命令时报连接错误。
"""

import asyncio
import json
import logging
import os
import re
import time
from collections import deque
from typing import Any, AsyncIterator, Dict, List, Optional

from astrbot.api import star
from astrbot.api.all import AstrMessageEvent, MessageChain
from astrbot.api.event import filter
from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path

from .dsh_api import DshBridgeClient, DshBridgeError

PREF_FILE = os.path.join(get_astrbot_plugin_data_path(), "dsh_harness_prefs.json")

logger = logging.getLogger("astrbot")

HELP_TEXT = """DSH 远程操作（白名单用户，私聊）：
· dsh <内容> — 交给 DeepSeek Harness 执行，返回非思考输出
· dsh 工作区 — 列出工作区；dsh 工作区 <编号|路径> 切换（新建会话）
· dsh 模型 — 列出模型（全局编号）；dsh 模型 <编号> 或 dsh 模型 <provider> <model> 切换
· dsh 新会话 / 状态 / 历史 / 停止 / 帮助
· DSH 提问时：dsh <答案>；请求授权时：dsh 同意 / dsh 拒绝"""

RESET_WORDS = {"新会话", "重置", "reset", "new", "clear"}
STATUS_WORDS = {"状态", "status", "s"}
HISTORY_WORDS = {"历史", "history", "log"}
STOP_WORDS = {"停止", "stop", "cancel"}
WORKSPACE_WORDS = {"工作区", "workspace", "ws"}
MODEL_WORDS = {"模型", "model"}
HELP_WORDS = {"帮助", "help", "h", "?"}
ALLOW_WORDS = {"同意", "允许", "批准", "确认", "可以", "yes", "y", "ok", "on"}


class DshHarnessPlugin(star.Star):
    def __init__(self, context, config=None):
        super().__init__(context, config)
        self.config = config or {}
        # user_id → asyncio.Task (流式任务)
        self._tasks: Dict[str, asyncio.Task] = {}
        # user_id → 待执行消息队列
        self._queue: Dict[str, deque] = {}
        # user_id → 等待用户应答的交互 (question/approval)
        self._pending: Dict[str, Dict[str, Any]] = {}
        # user_id → sessionId (本地缓存, 权威以 bridge 存储为准)
        self._sessions: Dict[str, str] = {}
        # user_id → 用户级偏好 (workspacePath / modelProvider / modelName / reasoningEffort)
        self._prefs: Dict[str, Dict[str, str]] = {}
        try:
            with open(PREF_FILE, "r", encoding="utf-8") as fp:
                loaded = json.load(fp)
                if isinstance(loaded, dict):
                    self._prefs = {k: v for k, v in loaded.items() if isinstance(v, dict)}
        except (OSError, ValueError):
            self._prefs = {}

    def _model_pairs(self, data: Dict[str, Any]) -> List[Dict[str, str]]:
        """把模型目录展平成 (provider, model) 列表, 供编号/解析使用。"""
        pairs: List[Dict[str, str]] = []
        for g in data.get("groups") or []:
            provider = str(g.get("provider") or "")
            for m in g.get("models") or []:
                pairs.append({
                    "provider": provider,
                    "providerName": str(g.get("name") or provider),
                    "model": str(m.get("id") or ""),
                    "modelName": str(m.get("name") or ""),
                })
        return pairs

    async def _fetch_model_catalog(self) -> List[Dict[str, str]]:
        """带 5 分钟缓存的模型目录 (展平列表)。"""
        now = time.monotonic()
        cached = getattr(self, "_model_cache", None)
        if cached and now - cached.get("ts", 0) < 300:
            return cached.get("pairs") or []
        data = await self._new_client().models()
        pairs = self._model_pairs(data)
        self._model_cache = {"ts": now, "pairs": pairs}
        return pairs

    def _resolve_model(
        self, pairs: List[Dict[str, str]], provider: str, model: str
    ) -> Optional[Dict[str, str]]:
        """把用户输入的 provider/model 解析成唯一 (provider, model)。

        规则: provider 先精确匹配 id, 再唯一子串匹配; model 先精确匹配,
        再唯一子串匹配。跨提供商重名或匹配不到时返回 None。
        """
        provider = (provider or "").strip().lower()
        model = (model or "").strip().lower()
        if not provider or not model:
            return None
        providers = {p["provider"].lower() for p in pairs}
        resolved_provider = None
        if provider in providers:
            resolved_provider = provider
        else:
            cands = [p for p in providers if provider in p]
            if len(cands) == 1:
                resolved_provider = cands[0]
            else:
                return None
        in_provider = [p for p in pairs if p["provider"].lower() == resolved_provider]
        exact = [p for p in in_provider if p["model"].lower() == model]
        if len(exact) == 1:
            return exact[0]
        sub = [p for p in in_provider if model in p["model"].lower()]
        if len(sub) == 1:
            return sub[0]
        return None

    def _save_prefs(self) -> None:
        try:
            os.makedirs(os.path.dirname(PREF_FILE), exist_ok=True)
            with open(PREF_FILE, "w", encoding="utf-8") as fp:
                json.dump(self._prefs, fp, ensure_ascii=False, indent=2)
        except OSError as e:
            logger.warning(f"保存偏好失败: {e}")

    # ------------------------------------------------------------------
    # 配置 / 权限 / 发送辅助
    # ------------------------------------------------------------------
    def _cfg(self, key: str, default: Any = None) -> Any:
        value = self.config.get(key, default)
        return default if value is None else value

    def _new_client(self) -> DshBridgeClient:
        return DshBridgeClient(
            base_url=self._cfg("dsh_base_url", "http://127.0.0.1:3080"),
            token=self._cfg("dsh_bot_token", ""),
            timeout=self._cfg("http_timeout", 30),
        )

    def _check_permission(self, event: AstrMessageEvent) -> Optional[str]:
        """返回 None 表示放行, 否则返回拒绝原因。"""
        if not bool(self._cfg("dsh_enabled", True)):
            return "DSH 远程功能当前已关闭（dsh_enabled=false）。"
        sender_id = str(event.get_sender_id() or "").strip()
        allowed = {
            str(x).strip()
            for x in (self._cfg("allowed_user_ids") or [])
            if str(x).strip()
        }
        if not allowed:
            return "插件尚未配置 allowed_user_ids 白名单，任何用户都不能使用 DSH 远程。"
        if sender_id not in allowed:
            return "你没有 DSH 远程操作权限，请联系管理员把 QQ 号加入 allowed_user_ids。"
        if not bool(self._cfg("allow_group_chat", False)) and not event.is_private_chat():
            return "DSH 远程仅支持私聊，请私聊机器人使用。"
        return None

    async def _safe_send(self, event: AstrMessageEvent, chain: MessageChain) -> bool:
        try:
            await event.send(chain)
            return True
        except Exception as e:
            logger.warning(f"发送消息失败: {e}")
            return False

    async def _send_text(self, event: AstrMessageEvent, text: str) -> None:
        """按 max_message_len 切块发送长文本 (尽量在换行处断开)。"""
        if not text:
            return
        max_len = max(500, int(self._cfg("max_message_len", 3000) or 3000))
        remaining = text
        while remaining:
            if len(remaining) <= max_len:
                await self._safe_send(event, MessageChain().message(remaining))
                return
            cut = remaining.rfind("\n", 0, max_len)
            if cut <= max_len // 2:
                cut = max_len
            await self._safe_send(event, MessageChain().message(remaining[:cut]))
            remaining = remaining[cut:].lstrip("\n")

    def _client_id(self, event: AstrMessageEvent) -> str:
        return "qq:" + str(event.get_sender_id() or "").strip()

    def _is_busy(self, user_id: str) -> bool:
        task = self._tasks.get(user_id)
        return task is not None and not task.done()

    # ------------------------------------------------------------------
    # 命令入口
    # ------------------------------------------------------------------
    @filter.command("dsh")
    async def cmd_dsh(self, event: AstrMessageEvent):
        async for r in self._cmd_dsh_common(event, "dsh"):
            yield r

    @filter.command("/dsh")
    async def cmd_dsh_slash(self, event: AstrMessageEvent):
        async for r in self._cmd_dsh_common(event, "/dsh"):
            yield r

    async def _cmd_dsh_common(
        self, event: AstrMessageEvent, keyword: str
    ) -> AsyncIterator[Any]:
        denied = self._check_permission(event)
        if denied:
            event.stop_event()
            yield event.make_result().message(denied)
            return
        body = (event.get_message_str() or "").replace(keyword, "", 1).strip()
        if not body:
            body = "帮助"
        sender = str(event.get_sender_id() or "")
        lower = body.lower()

        if body in HELP_WORDS:
            event.stop_event()
            yield event.make_result().message(HELP_TEXT)
            return

        if body in RESET_WORDS:
            event.stop_event()
            try:
                client = self._new_client()
                await client.reset(self._client_id(event))
            except DshBridgeError as e:
                yield event.make_result().message(f"❌ 重置会话失败：{e}")
                return
            self._pending.pop(sender, None)
            self._sessions.pop(sender, None)
            busy_hint = ""
            if self._is_busy(sender):
                busy_hint = "（当前有任务在执行，将按旧会话继续跑完；下一条新消息会创建新会话）"
            yield event.make_result().message(f"✅ 已重置 DSH 会话{busy_hint}")
            return

        if body in STOP_WORDS:
            event.stop_event()
            session_id = self._sessions.get(sender)
            if not session_id:
                try:
                    session_id = await self._new_client().get_session(self._client_id(event))
                except DshBridgeError as e:
                    yield event.make_result().message(f"❌ 停止失败：{e}")
                    return
            if not session_id:
                yield event.make_result().message("当前没有 DSH 会话，无需停止。")
                return
            try:
                await self._new_client().cancel(session_id)
                yield event.make_result().message("🛑 已请求 DSH 打断当前任务。")
            except DshBridgeError as e:
                yield event.make_result().message(f"❌ 停止失败：{e}")
            return

        if body in WORKSPACE_WORDS or body.startswith(("工作区 ", "workspace ", "ws ")):
            event.stop_event()
            try:
                client = self._new_client()
                data = await client.workspaces()
            except DshBridgeError as e:
                yield event.make_result().message(f"❌ 读取工作区失败：{e}")
                return
            items = data.get("items") or []
            parts = body.split(None, 1)
            arg = parts[1].strip() if len(parts) > 1 else ""
            if not arg:
                if not items:
                    yield event.make_result().message("DSH 当前没有注册任何工作区（在 DSH 里打开/创建一个项目即可）。")
                    return
                lines = ["🗂 DSH 工作区："]
                for idx, it in enumerate(items):
                    lines.append(f"{idx + 1}. {it.get('title') or '未命名'} — {it.get('path')}")
                pref = self._prefs.get(sender, {}).get("workspacePath")
                if pref:
                    lines.append(f"当前偏好：{pref}")
                lines.append("切换：dsh 工作区 <编号|路径>（会新建会话）")
                yield event.make_result().message("\n".join(lines))
                return
            path = ""
            if arg.isdigit() and 1 <= int(arg) <= len(items):
                path = str(items[int(arg) - 1].get("path") or "")
            else:
                path = arg
            if not path:
                yield event.make_result().message("未找到该工作区，用「dsh 工作区」查看列表。")
                return
            self._prefs.setdefault(sender, {})["workspacePath"] = path
            self._save_prefs()
            try:
                await self._new_client().reset(self._client_id(event))
            except DshBridgeError:
                pass
            self._sessions.pop(sender, None)
            yield event.make_result().message(f"✅ 已切换工作区：{path}\n下一条 dsh 消息将在该工作区新建会话。")
            return

        if body in MODEL_WORDS or body.startswith(("模型 ", "model ")):
            event.stop_event()
            parts = body.split()
            try:
                pairs = await self._fetch_model_catalog()
            except DshBridgeError as e:
                yield event.make_result().message(f"❌ 读取模型目录失败：{e}")
                return
            if len(parts) == 1:
                if not pairs:
                    yield event.make_result().message("DSH 模型目录为空。")
                    return
                lines = ["🧠 DSH 模型目录（全局编号，重名也不怕）："]
                for idx, p in enumerate(pairs[:50]):
                    lines.append(f"{idx + 1}. {p['provider']} / {p['model']}（{p['modelName']}）")
                if len(pairs) > 50:
                    lines.append(f"…共 {len(pairs)} 个，仅显示前 50 个")
                pref = self._prefs.get(sender, {})
                if pref.get("modelProvider") and pref.get("modelName"):
                    lines.append(f"当前偏好：{pref.get('modelProvider')}/{pref.get('modelName')}")
                lines.append("切换：dsh 模型 <编号>；或 dsh 模型 <provider> <model> [思考强度]")
                yield event.make_result().message("\n".join(lines))
                return

            arg1 = parts[1].strip()
            effort = parts[2].strip() if len(parts) > 2 else ""
            provider = ""
            model = ""

            if arg1.isdigit():
                # 全局编号 → 唯一 (provider, model)
                index = int(arg1) - 1
                if 0 <= index < len(pairs):
                    provider = pairs[index]["provider"]
                    model = pairs[index]["model"]
                else:
                    yield event.make_result().message(f"编号超出范围（1-{len(pairs)}），用「dsh 模型」查看。")
                    return
            elif len(parts) >= 3:
                provider_arg, model_arg = arg1, parts[2].strip()
                resolved = self._resolve_model(pairs, provider_arg, model_arg)
                if not resolved:
                    cands = [p for p in pairs if p["model"].lower() == model_arg.lower()]
                    if len(cands) > 1:
                        lines = [f"多个提供商都有模型「{model_arg}」，请用编号："]
                        for idx, p in enumerate(pairs):
                            if p["model"].lower() == model_arg.lower():
                                lines.append(f"{idx + 1}. {p['provider']} / {p['model']}（{p['modelName']}）")
                        yield event.make_result().message("\n".join(lines))
                    else:
                        yield event.make_result().message(
                            f"没有解析到模型「{provider_arg} / {model_arg}」。用「dsh 模型」查看编号后输入 dsh 模型 <编号>。"
                        )
                    return
                provider, model = resolved["provider"], resolved["model"]
            else:
                # 只给了一个模型名: 全目录唯一匹配 (先精确, 再子串)
                exact = [p for p in pairs if p["model"].lower() == arg1.lower()]
                if len(exact) == 1:
                    provider, model = exact[0]["provider"], exact[0]["model"]
                else:
                    sub = [p for p in pairs if arg1.lower() in p["model"].lower()]
                    if len(sub) == 1:
                        provider, model = sub[0]["provider"], sub[0]["model"]
                    else:
                        cands = exact if exact else sub
                        if len(cands) > 1:
                            lines = [f"「{arg1}」匹配到多个模型，请用编号："]
                            for idx, p in enumerate(pairs):
                                if p in cands:
                                    lines.append(f"{idx + 1}. {p['provider']} / {p['model']}（{p['modelName']}）")
                            yield event.make_result().message("\n".join(lines))
                        else:
                            yield event.make_result().message(f"没有找到模型「{arg1}」。用「dsh 模型」查看编号。")
                        return

            prefs = self._prefs.setdefault(sender, {})
            prefs["modelProvider"] = provider
            prefs["modelName"] = model
            if effort:
                prefs["reasoningEffort"] = effort
            self._save_prefs()
            session_id = self._sessions.get(sender)
            if not session_id:
                try:
                    session_id = await self._new_client().get_session(self._client_id(event))
                except DshBridgeError:
                    session_id = None
            if session_id:
                try:
                    await self._new_client().select_model(session_id, provider, model, effort or None)
                    yield event.make_result().message(f"✅ 当前会话已切换模型：{provider}/{model}" + (f"（思考强度 {effort}）" if effort else ""))
                except DshBridgeError as e:
                    yield event.make_result().message(f"❌ 切换模型失败：{e}（偏好已保存，新建会话时生效）")
                return
            yield event.make_result().message(f"✅ 已保存模型选择：{provider}/{model}，下一条消息新建会话时生效。")
            return

        if body in STATUS_WORDS:
            event.stop_event()
            try:
                session_id = await self._new_client().get_session(self._client_id(event))
            except DshBridgeError as e:
                yield event.make_result().message(f"❌ 查询状态失败：{e}")
                return
            if session_id:
                self._sessions[sender] = session_id
            busy = self._is_busy(sender)
            queued = len(self._queue.get(sender, ()))
            pending = self._pending.get(sender)
            lines = [
                "DSH 状态：",
                f"· 会话：{session_id or '无（下一条消息自动创建）'}",
                f"· 状态：{'执行中' if busy else '空闲'}",
                f"· 排队消息：{queued} 条",
            ]
            if pending and pending.get("kind") == "question":
                lines.append("· 等待你回答 DSH 的提问")
            elif pending and pending.get("kind") == "approval":
                lines.append("· 等待你决定是否授权工具")
            yield event.make_result().message("\n".join(lines))
            return

        if body in HISTORY_WORDS:
            event.stop_event()
            try:
                client = self._new_client()
                session_id = await client.get_session(self._client_id(event))
                if not session_id:
                    yield event.make_result().message("还没有 DSH 会话，先发一条 dsh <内容> 开始吧。")
                    return
                data = await client.history(
                    session_id, int(self._cfg("history_limit", 20) or 20)
                )
            except DshBridgeError as e:
                yield event.make_result().message(f"❌ 读取历史失败：{e}")
                return
            items = data.get("items") or []
            if not items:
                yield event.make_result().message("会话暂无文本输出。")
                return
            lines: List[str] = ["📜 最近输出："]
            for item in items:
                role = "你" if item.get("role") == "user" else "DSH"
                text = str(item.get("text") or "").strip()
                if not text:
                    continue
                if len(text) > 2000:
                    text = text[:2000] + "…(过长截断)"
                lines.append(f"【{role}】{text}")
            yield event.make_result().message("\n\n".join(lines))
            return

        # 等待中的提问/授权应答优先
        if sender in self._pending:
            event.stop_event()
            asyncio.create_task(self._handle_answer(event, body))
            yield event.make_result().message("📤 正在提交…")
            return

        # 普通任务
        event.stop_event()
        if self._is_busy(sender):
            queued = self._queue.setdefault(sender, deque(maxlen=10))
            if len(queued) >= 10:
                yield event.make_result().message("⚠️ 排队已满（10 条），请稍后再发。")
                return
            queued.append(body)
            yield event.make_result().message(
                f"⏳ 上一条 DSH 任务仍在执行，本条已排队（第 {len(queued)} 条），完成后自动继续。"
            )
            return
        task = asyncio.create_task(self._run_prompt(event, body))
        self._tasks[sender] = task
        yield event.make_result().message("🤖 已提交给 DSH，输出会持续发到这里…")

    # ------------------------------------------------------------------
    # 任务执行 (SSE 流 → QQ)
    # ------------------------------------------------------------------
    async def _run_prompt(self, event: AstrMessageEvent, text: str) -> None:
        sender = str(event.get_sender_id() or "")
        client = self._new_client()
        client_id = self._client_id(event)
        buffer: List[str] = []
        last_flush = time.monotonic()

        async def flush() -> None:
            nonlocal buffer, last_flush
            if not buffer:
                return
            chunk = "".join(buffer)
            buffer = []
            last_flush = time.monotonic()
            await self._send_text(event, chunk)

        prefs = self._prefs.get(sender, {})
        workspace_path = (prefs.get("workspacePath") or "").strip() or str(self._cfg("workspace_path", "") or "").strip()
        provider = (prefs.get("modelProvider") or "").strip() or str(self._cfg("model_provider", "") or "").strip()
        model = (prefs.get("modelName") or "").strip() or str(self._cfg("model_name", "") or "").strip()
        effort = (prefs.get("reasoningEffort") or "").strip() or str(self._cfg("reasoning_effort", "") or "").strip()
        # 面板/偏好里的模型字符串同样做目录解析 (提供商缩写、大小写、重名都安全)
        if provider and model:
            try:
                resolved = self._resolve_model(await self._fetch_model_catalog(), provider, model)
                if resolved:
                    provider, model = resolved["provider"], resolved["model"]
                else:
                    logger.warning(f"模型配置无法解析: {provider}/{model}, 将原样传给 bridge")
            except DshBridgeError:
                pass
        try:
            async for ev in client.prompt_stream(
                client_id=client_id,
                text=text,
                workspace_path=workspace_path or None,
                model_provider=provider or None,
                model_name=model or None,
                reasoning_effort=effort or None,
            ):
                etype = ev.get("type")
                if etype == "text":
                    buffer.append(str(ev.get("text") or ""))
                    joined = "".join(buffer)
                    now = time.monotonic()
                    flush_chars = int(self._cfg("stream_flush_chars", 2000) or 2000)
                    min_chars = int(self._cfg("stream_min_chars", 300) or 300)
                    interval = float(self._cfg("stream_flush_interval", 3.0) or 3.0)
                    if len(joined) >= flush_chars or (
                        now - last_flush >= interval and len(joined) >= min_chars
                    ):
                        await flush()
                elif etype == "tool":
                    await flush()
                    name = str(ev.get("name") or "").strip()
                    status = ev.get("status")
                    if status == "call" and bool(self._cfg("show_tool_calls", True)) and name:
                        await self._safe_send(event, MessageChain().message(f"🔧 {name}"))
                    elif status == "result" and bool(self._cfg("show_tool_results", False)) and name:
                        await self._safe_send(event, MessageChain().message(f"✅ {name}"))
                elif etype == "question":
                    await flush()
                    self._pending[sender] = {
                        "kind": "question",
                        "sessionId": ev.get("sessionId") or self._sessions.get(sender, ""),
                        "rpcId": ev.get("rpcId"),
                        "questions": ev.get("questions") or [],
                    }
                    await self._safe_send(
                        event, MessageChain().message(self._format_questions(ev.get("questions") or []))
                    )
                elif etype == "approval":
                    await flush()
                    self._pending[sender] = {
                        "kind": "approval",
                        "sessionId": ev.get("sessionId") or self._sessions.get(sender, ""),
                        "rpcId": ev.get("rpcId"),
                        "approvalId": ev.get("approvalId"),
                        "toolName": ev.get("toolName") or "",
                    }
                    reason = str(ev.get("reason") or "").strip()
                    reason_text = f"\n原因：{reason}" if reason else ""
                    await self._safe_send(
                        event,
                        MessageChain().message(
                            f"⚠️ DSH 请求授权执行工具【{ev.get('toolName') or 'unknown'}】{reason_text}\n"
                            "回复「dsh 同意」放行一次，「dsh 拒绝」拒绝。",
                        ),
                    )
                elif etype == "error":
                    await flush()
                    await self._safe_send(
                        event, MessageChain().message(f"❌ DSH：{ev.get('message') or '未知错误'}")
                    )
                elif etype == "status" and ev.get("status") == "accepted":
                    session_id = ev.get("sessionId")
                    if session_id:
                        self._sessions[sender] = session_id
                elif etype == "done":
                    break
            await flush()
        except asyncio.CancelledError:
            await flush()
            raise
        except DshBridgeError as e:
            await flush()
            await self._safe_send(event, MessageChain().message(f"❌ DSH 桥接失败：{e}"))
        except Exception as e:
            logger.error(f"DSH 任务异常: {e}", exc_info=True)
            await flush()
            await self._safe_send(event, MessageChain().message(f"❌ DSH 任务异常：{e}"))
        finally:
            self._tasks.pop(sender, None)
            # 无交互挂起时自动消费排队消息
            if sender not in self._pending:
                queued = self._queue.get(sender)
                next_text = queued.popleft() if queued else None
                if not queued:
                    self._queue.pop(sender, None)
                if next_text:
                    task = asyncio.create_task(self._run_prompt(event, next_text))
                    self._tasks[sender] = task

    def _format_questions(self, questions: List[Dict[str, Any]]) -> str:
        if not questions:
            return "🤔 DSH 需要你的输入，请用「dsh <答案>」回复。"
        lines = ["🤔 DSH 需要你的输入："]
        for idx, q in enumerate(questions):
            header = str(q.get("header") or "").strip()
            if header:
                lines.append(f"【{header}】")
            lines.append(f"{idx + 1}. {q.get('question') or ''}")
            detail = str(q.get("detail") or "").strip()
            if detail:
                lines.append(f"   {detail[:500]}")
            options = q.get("options") or []
            if options:
                opts = " / ".join(
                    f"{i + 1}) {o.get('label') if isinstance(o, dict) else o}"
                    for i, o in enumerate(options[:8])
                )
                lines.append(f"   选项：{opts}")
                if q.get("multiSelect"):
                    lines.append("   （可多选）")
        lines.append("用「dsh <答案>」回复（如 dsh 1 表示选第 1 项）。")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 提问 / 授权应答
    # ------------------------------------------------------------------
    async def _handle_answer(self, event: AstrMessageEvent, text: str) -> None:
        sender = str(event.get_sender_id() or "")
        pending = self._pending.pop(sender, None)
        if not pending:
            return
        client = self._new_client()
        try:
            if pending.get("kind") == "question":
                questions: List[Dict[str, Any]] = pending.get("questions") or []
                answers: List[Dict[str, Any]] = []
                for idx, q in enumerate(questions):
                    qid = str(q.get("id") or f"q{idx}")
                    if idx == 0:
                        selected = self._match_option(q, text)
                        if selected:
                            answers.append({"id": qid, "selected": selected, "custom": ""})
                        else:
                            answers.append({"id": qid, "selected": [], "custom": text})
                    else:
                        answers.append({"id": qid, "selected": []})
                await client.answer_question(
                    str(pending.get("sessionId") or ""), str(pending.get("rpcId") or ""), answers
                )
                await self._safe_send(
                    event, MessageChain().message("📤 答案已提交，DSH 继续执行，输出随后到达…")
                )
            elif pending.get("kind") == "approval":
                outcome = "allowed-once" if text.strip().lower() in ALLOW_WORDS else "rejected"
                await client.approve(
                    str(pending.get("sessionId") or ""),
                    str(pending.get("rpcId") or ""),
                    str(pending.get("approvalId") or ""),
                    outcome,
                )
                label = "已放行本次授权" if outcome == "allowed-once" else "已拒绝本次授权"
                await self._safe_send(
                    event, MessageChain().message(f"🔐 {label}，DSH 继续执行…")
                )
        except DshBridgeError as e:
            await self._safe_send(event, MessageChain().message(f"❌ 提交失败：{e}"))

    @staticmethod
    def _match_option(question: Dict[str, Any], text: str) -> Optional[List[str]]:
        """用户输入选项编号或选项文本时返回 selected 列表, 否则 None。"""
        options = question.get("options") or []
        if not options:
            return None
        labels: List[str] = []
        for o in options:
            label = o.get("label") if isinstance(o, dict) else str(o)
            labels.append(str(label))
        stripped = text.strip()
        if re.fullmatch(r"\d+", stripped):
            index = int(stripped) - 1
            if 0 <= index < len(labels):
                return [labels[index]]
        if stripped in labels:
            return [stripped]
        return None