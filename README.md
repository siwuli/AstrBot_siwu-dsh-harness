# 兔兔 - AstrBot DSH 远程控制插件

指定**白名单用户**在**私聊**中通过机器人接入 [DeepSeek Harness (DSH)](https://github.com/deepseek-ai/deepseek-harness) 执行任务：
用户消息 → 插件 → dsh-bot-bridge (DSH 宿主插件) → DSH Agent（可调用工具/子代理等）→ **非思考输出**实时流式回到 QQ。

> 插件 id：dsh_harness　当前版本：1.0.0

## 前置条件

1. 一台运行 DSH Web 的机器（本机或局域网/服务器均可），并安装宿主插件 [dsh-bot-bridge](https://github.com/siwuli/dsh-bot-bridge)（npm 包同名）：

    dsh plugin --profile <profile> add dsh-bot-bridge

2. 在 DSH profile 的 cordis.patch.yml 里配置 botToken（与下方插件配置一致）：

    - id: bot-bridge
      config:
        botToken: '一个随机长令牌'

3. 若 DSH 与 AstrBot 不在同一台机器：DSH 需以 dsh web --host 0.0.0.0 启动（或经隧道暴露），并在本插件 dsh_base_url 里填局域网/公网地址。

## 功能

- 白名单 + 默认仅私聊：只有 allowed_user_ids 里的用户、且在私聊中才可用；
- 实时流式返回 harness 的**非思考输出**（reasoning/thinking 在 bridge 端已过滤），正文按配置攒批发送、长文本自动切块；
- 多轮会话：每个 QQ 用户绑定一个 DSH 会话（clientId 机制），对话上下文持久；
- 工具动态：harness 调用工具时推送一行提示（可关）；
- 交互回传：DSH 的 ask_user_question 提问、工具授权请求会转发到 QQ，用户回复后继续执行；
- 任务排队：上一条任务执行中发新消息自动排队，完成后自动继续；
- 历史/状态/停止/新会话等管理命令。

## 使用

    dsh <内容>           把内容交给 DSH 执行（首次自动创建会话）
    dsh 新会话 / reset   解绑会话，下条消息自动新建
    dsh 状态 / status    查看会话与执行状态
    dsh 历史 / history   查看最近输出（默认排除思考）
    dsh 停止 / stop      打断当前任务
    dsh 帮助 / help      使用说明

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
| stream_flush_interval | 输出刷新间隔（秒） | 0.8 |
| stream_flush_chars | 攒够多少字符立即发送 | 800 |
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
| 1.0.0 | 初版：白名单/私聊权限、SSE 非思考输出流式转发、多轮会话、提问/授权应答、排队、历史/状态/停止/新会话命令 |

## 项目地址

<https://github.com/siwuli/AstrBot_siwu-dsh-harness>

