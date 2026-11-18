# 实时计算服务中心

面向实时数据分析场景，提供流式计算定义、窗口聚合、策略告警和状态管理的实时数据平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式源端 | `stream_source` | 数据源配置 | 源端名称、消息队列、Topic名称 |
| 窗口配置 | `window_config` | 窗口策略 | 策略名称、窗口类型、窗口大小 |
| 计算算子 | `calc_operator` | 算子定义 | 算子名称、算子类型、输入字段 |
| 状态存储 | `state_store` | 状态配置 | 配置名称、后端类型、状态键模式 |
| 水位追踪 | `watermark_track` | 水位记录 | 水位名称、数据源、当前水位 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控指标、触发条件 |
| 滑动指标 | `sliding_index` | 指标计算 | 指标名称、计算窗口、聚合函数 |
| 流关联 | `join_stream` | 关联配置 | 配置名称、左流源、右流源 |
| 结果落地 | `sink_output` | 输出配置 | 配置名称、输出类型、目标地址 |
| 补数任务 | `backfill_job` | 补数作业 | 作业名称、目标指标、补数范围 |
| CEP规则 | `cep_rule` | 事件序列 | 序列名称、事件类型、匹配顺序 |
| 吞吐管理 | `throughput_mgr` | 吞吐配置 | 配置名称、数据源、最大QPS |
| 检查点 | `checkpoint_mgr` | 检查点配置 | 配置名称、间隔时间、超时时长 |
| 故障恢复 | `restore_plan` | 恢复方案 | 方案名称、任务范围、恢复模式 |
| 指标血缘 | `metric_lineage` | 血缘记录 | 记录ID、指标名称、上游源端 |
| 迟到处理 | `late_handler` | 处理策略 | 策略名称、允许延迟、迟到动作 |
| 任务拓扑 | `task_topology` | 拓扑配置 | 拓扑名称、算子链、并行度 |
| 数据采样 | `data_sampler` | 采样策略 | 策略名称、采样方式、采样比例 |
| 调试追踪 | `debug_trace` | 调试会话 | 会话名称、目标算子、采样数据 |
| 聚合落库 | `aggregate_sink` | 聚合任务 | 任务名称、源指标集、聚合粒度 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 序号33 · 指标过期清理与窗口引用检查

旧版过期清理只看保留时长，过期后窗口指标数据失去计算口径。现在：

- `sliding_index.expire_old_indexes(run_key=...)` 清理前先读取指标引用
  （窗口指标数据 + 窗口策略绑定 + 进行中补算）：被引用的到期指标保留定义、
  归档版本并把状态写入迟到补算清单（`backfill_job`）；引用全部解除后才停用。
- 历史结果保留指标版本：定义快照写入 `_archived_versions`，窗口指标数据
  携带 `_metric_version` 来源版本，过期指标的历史结果仍可解释。
- 重算同一窗口只能使用仍有效的指标定义；保留到期与重算同时到达时二选一：
  `extend` 延长保留直到窗口发布完成，`archive` 复制版本到归档后立即清理，
  两种方案都保证当前可见结果只保留一个可解释口径。
- 窗口配置模块 `sync_expired_metric_refs()` 读取过期指标清单，窗口仍引用时
  保留定义并更新迟到补算清单，补齐窗口指标数据的来源版本。
- 专用接口（注册在通用资源路由之前）：
  - `POST /api/sliding_index/cleanup/run` 过期清理（同 run_key 幂等重放）
  - `GET /api/sliding_index/{id}/references` 指标页引用窗口视图
  - `GET /api/sliding_index/{id}/window_metrics` 序号33查询接口
    （筛选/检索/排序/分页，并发查询只返回一版，同参数返回同一结果）
  - `POST /api/sliding_index/{id}/recalculate` 窗口重算（extend/archive）
  - `POST /api/sliding_index/{id}/release` 解除窗口结果引用
  - `POST /api/window_config/sync_metric_refs` 窗口侧同步过期指标引用
  - `GET /api/window_config/metric_version_confirmation` 窗口页版本确认
- 清理/重算/同步均为跨模块事务：任一环节失败恢复所有原记录。
- 前端：滑动指标页内嵌「序号33」面板（引用窗口、归档版本、窗口指标数据
  分页详情、延期/归档重算），窗口配置页提供引用同步与版本确认；筛选条件
  同步到 URL query，跨页面定位同一筛选结果。

领域：数据工程
子方向：数据分析与数据应用
