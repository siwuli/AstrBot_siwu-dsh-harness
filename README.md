# DSH远程控制（dsh_harness）

白名单用户在私聊中让机器人接入 DSH 执行任务，流式回传正文与工具动态。

## 前置条件

- 一台运行 DSH Web 的机器，并安装宿主插件 dsh-bot-bridge（npm 包同名）；
- 在 DSH profile 的 cordis.patch.yml 里配置 botToken，与插件配置 dsh_bot_token 一致；
- 网络：同机填 `http://127.0.0.1:3080`；已有反代填反代地址；跨机可用 SSH 反向隧道后填 `http://127.0.0.1:13080`。

## 功能

- 白名单 + 默认仅私聊：只有 allowed_user_ids 中的用户在私聊里可用；
- 实时流式回传 DSH 的非思考输出（正文与工具动态），长文本自动切块；
- 每个用户绑定一个 DSH 会话，多轮上下文持久，会话挂载到对应工作区；
- 工作区与模型可在面板配默认值，也可用命令随时切换；
- DSH 的提问与工具授权请求转发到 QQ，回复后继续执行；
- 任务排队、历史查看、状态查询、任务停止、新建会话。

## 指令

| 指令 | 说明 |
| --- | --- |
| `dsh <内容>` | 交给 DSH 执行，首次自动创建会话 |
| `dsh 工作区 [编号\|路径]` | 列出工作区，或切换（下条消息新建会话） |
| `dsh 模型 [编号]` / `dsh 模型 <provider> <model>` | 列出或切换模型 |
| `dsh 会话 [工作区编号] [会话编号]` | 查看工作区会话，或继续历史会话 |
| `dsh 新会话` / `状态` / `历史` / `停止` / `帮助` | 会话管理与使用说明 |
| `dsh <答案>`、`dsh 同意` / `dsh 拒绝` | 回答提问、应答工具授权 |

## 配置

| 配置项 | 默认 | 说明 |
| --- | --- | --- |
| dsh_enabled | true | 插件总开关 |
| dsh_base_url | http://127.0.0.1:3080 | dsh-bot-bridge 地址 |
| dsh_bot_token | 空（必填） | 与 bridge 的 botToken 一致 |
| allowed_user_ids | 空 | 可用用户 ID 白名单，每行一个 |
| allow_group_chat | false | 是否允许群聊使用 |
| workspace_path | 空 | 新会话默认工作区路径 |
| model_provider | 空 | 新会话默认模型 provider |
| model_name | 空 | 新会话默认模型 id |
| reasoning_effort | 空 | 默认思考强度 low/medium/high |
| stream_flush_interval | 3.0 | 输出攒批间隔（秒） |
| stream_flush_chars | 2000 | 攒够多少字符立即发送 |
| stream_min_chars | 300 | 最小发送字符数，防逐词刷屏 |
| max_message_len | 3000 | 单条 QQ 消息最大长度 |
| show_tool_calls | true | 推送工具调用提示 |
| show_tool_results | false | 推送工具完成提示 |
| history_limit | 20 | 历史命令返回条数 |
| http_timeout | 30 | HTTP 建连超时（秒） |

## 安装

1. 把插件目录放到 AstrBot 的 data/plugins/dsh_harness，或在插件管理里上传打包后的 zip；
2. 依赖 aiohttp（AstrBot 环境自带），无需额外安装；
3. 填好 dsh_base_url、dsh_bot_token、allowed_user_ids，私聊发送 `dsh 帮助` 验证。

## 版本记录

| 版本 | 说明 |
| --- | --- |
| 1.4.1 | 精简文档 |
| 1.4.0 | 会话改为先选工作区分级查看 |
| 1.3.1 | 会话列表按工作区过滤 |
| 1.3.0 | 支持切换并继续历史会话 |
| 1.2.1 | 工作区路径校验 |
