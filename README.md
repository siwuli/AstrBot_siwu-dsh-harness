# 兔兔 - AstrBot DSH 远程控制插件

指定**白名单用户**在**私聊**中通过机器人接入 [DeepSeek Harness (DSH)](https://github.com/deepseek-ai/deepseek-harness) 执行任务：
用户消息 → 插件 → dsh-bot-bridge (DSH 宿主插件) → DSH Agent（可调用工具/子代理等）→ **非思考输出**实时流式回到 QQ。

> 插件 id：dsh_harness　当前版本：1.3.0

## 前置条件

1. 一台运行 DSH Web 的机器（本机或局域网/服务器均可），并安装宿主插件 [dsh-bot-bridge](https://github.com/siwuli/dsh-bot-bridge)（npm 包同名）：

    dsh plugin --profile <你的profile名> add dsh-bot-bridge

2. 在 DSH profile 的 cordis.patch.yml 里配置 botToken（与下方插件配置一致）：

    - id: bot-bridge
      config:
        botToken: '一个随机长令牌'

3. 网络拓扑：
   - 默认（推荐）：DSH 与 AstrBot 同机 → dsh_base_url 填 http://127.0.0.1:3080，零额外配置；
   - 已有反向代理（Caddy/Nginx）：DSH 已通过反代暴露域名时直接填该地址（如 https://dsh.example.com），bridge 自带令牌认证；
   - 跨机：部分 DSH 版本 CLI 禁止 0.0.0.0 绑定，推荐在 DSH 所在机器用 SSH 反向隧道
     （ssh -N -R 13080:127.0.0.1:3080 <机器人机器>），AstrBot 填 http://127.0.0.1:13080；
     或在机器人同机部署一套 DSH。

## 功能

- 白名单 + 默认仅私聊：只有 allowed_user_ids 里的用户、且在私聊中才可用；
- 实时流式返回 harness 的**非思考输出**（reasoning/thinking 在 bridge 端已过滤），正文攒批发送（默认攒 300 字/3 秒才发，不再逐词刷屏）、长文本自动切块；
- 多轮会话：每个 QQ 用户绑定一个 DSH 会话（clientId 机制），对话上下文持久；会话会挂载到对应 DSH 工作区项目下（Web GUI 可见）；
- 工作区/模型选择：面板可配默认值，也可用 dsh 工作区 / dsh 模型 命令随时切换；
- 工具动态：harness 调用工具时推送一行提示（可关）；
- 交互回传：DSH 的 ask_user_question 提问、工具授权请求会转发到 QQ，用户回复后继续执行；
- 任务排队：上一条任务执行中发新消息自动排队，完成后自动继续；
- 历史/状态/停止/新会话等管理命令。

## 使用

    dsh <内容>                    把内容交给 DSH 执行（首次自动创建会话）
    dsh 工作区 / workspace         列出工作区
    dsh 工作区 <编号|路径>         切换工作区（下条消息新建会话）
    dsh 模型 / model              列出模型目录（全局编号，重名不怕）
    dsh 模型 <编号>               按编号切换（有会话立即生效，无会话则新建时生效）
    dsh 模型 <provider> <model>   按名称切换（支持缩写/子串唯一匹配，歧义会列出候选）
    dsh 会话 / sessions           列出历史会话
    dsh 会话 <编号>               切换到历史会话继续对话
    dsh 新会话 / reset            解绑会话，下条消息自动新建
    dsh 状态 / status             查看会话与执行状态
    dsh 历史 / history            查看最近输出（默认排除思考）
    dsh 停止 / stop               打断当前任务
    dsh 帮助 / help               使用说明

- DSH 提问时直接回复：dsh <答案>（dsh 1 = 选第 1 项）；
- DSH 请求工具授权时回复：dsh 同意 / dsh 拒绝。

## 配置（AstrBot 面板 → 插件配置）

| 配置项 | 说明 | 默认 |
| --- | --- | --- |
| dsh_enabled | 插件总开关 | true |
| dsh_base_url | dsh-bot-bridge 地址（DSH Web 端口） | http://127.0.0.1:3080 |
| dsh_bot_token | 与 bridge 的 botToken 一致 | 空（必填） |
| allowed_user_ids | 可用用户 QQ 号白名单，每行一个 | [] |
| allow_group_chat | 是否允许群聊使用 | false |
| workspace_path | 新会话默认工作区路径（留空用 DSH 默认） | 空 |
| model_provider | 新会话默认模型 provider（如 deepseek） | 空 |
| model_name | 新会话默认模型 id（如 deepseek-chat） | 空 |
| reasoning_effort | 默认思考强度（low/medium/high） | 空 |
| stream_flush_interval | 输出攒批间隔（秒） | 3.0 |
| stream_flush_chars | 攒够多少字符立即发送 | 2000 |
| stream_min_chars | 最小发送字符数（低于此值继续攒，防逐词刷屏） | 300 |
| max_message_len | 单条 QQ 消息最大长度 | 3000 |
| show_tool_calls | 推送工具调用提示 | true |
| show_tool_results | 推送工具完成提示 | false |
| history_limit | 历史命令返回条数 | 20 |
| http_timeout | HTTP 建连超时（秒） | 30 |

## 架构与耦合边界

本插件（AstrBot 侧）与 dsh-bot-bridge（DSH 侧）只通过一纸 HTTP 契约（/api/bot/* + SSE 帧）交互：

- bridge 不可用时，本插件照常加载，仅在 dsh 命令执行时报连接错误；
- 本插件不可用时，bridge 与 DSH Web GUI 完全不受影响；
- 两个插件均可独立被其他客户端复用（bridge 可对接任何机器人后端，本插件可对接任何实现了该契约的桥）。

## 安装方法

1. 执行打包脚本，生成 plugins/astrbot/dist/siwu-dsh-harness-<版本号>.zip；
2. AstrBot 管理面板 → 插件管理 → 安装插件 → 上传该 zip（或把插件目录放到 AstrBot 的 data/plugins/dsh_harness 后重启）；
3. 在插件配置里填 dsh_base_url、dsh_bot_token、allowed_user_ids；
4. 私聊机器人发送 dsh 帮助 验证。

### 重新打包

    python plugins/astrbot/siwu-dsh-harness-1_0/build.py

## 依赖

- Python 包：aiohttp（AstrBot 环境自带，见 requirements.txt）

## 版本记录

| 版本 | 更新内容 |
| --- | --- |
| 1.3.0 | 历史会话切换：dsh 会话 列出全部历史会话（标题/工作区/时间/状态），dsh 会话 <编号> 绑定并继续该对话（自动同步工作区偏好，不会被自动换绑打断） |
| 1.2.1 | 工作区路径校验：相对路径自动按 basename 匹配已注册工作区（dsh-astrbot → D:\work\dsh-astrbot），匹配不到则不发送并告警，避免 DSH 502 |
| 1.2.0 | 模型选择重做：全局编号选择（dsh 模型 <编号>）+ provider/model 模糊解析（缩写/子串唯一匹配，跨提供商重名列出候选），面板配置的模型字符串同样自动解析 |
| 1.1.0 | 流式输出改为攒批（≥300 字或 ≥3 秒，结束一次性补齐，不再逐词发送）；新增工作区选择（dsh 工作区）与模型切换（dsh 模型）；面板新增 workspace_path / model_provider / model_name / reasoning_effort 配置 |
| 1.0.0 | 初版：白名单/私聊权限、SSE 非思考输出流式转发、多轮会话、提问/授权应答、排队、历史/状态/停止/新会话命令 |

## 项目地址

<https://github.com/siwuli/AstrBot_siwu-dsh-harness>