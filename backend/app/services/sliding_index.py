"""滑动指标领域服务：定义基于滑动窗口的实时指标计算规则。

增强能力: 条件过滤、快照频率校验、过期清理、指标聚合输出、跨模块指标查询

序号33 · 指标过期清理与窗口引用检查
-----------------------------------
现状缺陷：旧版过期清理只看保留时长，把指标标成「已过期」后，窗口指标数据
（sink_output 中的输出记录）仍能被页面查询，补算任务却找不到对应计算口径，
留下无法解释的窗口指标数据。

本模块实现的保留逻辑：
1. 清理前先读取指标引用（窗口指标数据 + 窗口策略绑定 + 进行中补算），
   过期指标只有在所有当前窗口结果解除引用后才能停用。
2. 历史结果需要保留指标版本：每次状态变化前把定义快照写入归档版本，
   窗口指标数据携带 ``_metric_version`` 来源版本，历史结果始终可解释。
3. 重算同一窗口只能使用仍有效的指标定义（已过期指标拒绝重算）。
4. 保留时长到期与窗口重算同时到达时，两种方案二选一：
   - ``extend``  延长保留直到窗口发布完成（定义继续可用，清理延迟）；
   - ``archive`` 复制指标版本到归档后立即清理（口径冻结在归档版本，
     重算需引用归档版本）。默认 auto 选择 archive，避免过期定义滞留。
   两种方案都保证当前可见结果只保留一个可解释口径。
5. 序号33查询接口：并发查询只返回一版（被取代的结果不进结果集）、
   幂等（同 run_key 清理只执行一次、同参数查询返回同一结果）、
   事务失败恢复原记录。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SlidingIndexService(BaseModuleService):
    MODULE = "sliding_index"
    ENTITY = "指标计算"
    LIST_FIELDS = ["指标名称", "计算窗口", "聚合函数", "分组维度", "过滤条件", "快照频率", "保留时长"]
    REQUIRED_FIELDS = ["指标名称", "计算窗口", "聚合函数"]
    STATUS_ORDER = ["草稿", "计算中", "已生效", "已过期"]
    ACTION_RULES = {"启用指标": "草稿", "停用指标": "计算中"}
    NEGATIVE_ACTIONS = ["停用指标"]

    # 仍有效的指标定义状态：只有这些状态允许重算同一窗口
    VALID_STATUSES = ("草稿", "计算中", "已生效")
    DEFAULT_RETENTION_HOURS = 720
    # extend 策略的延长时长：延长保留直到窗口发布完成（24h 兜底）
    RETENTION_EXTEND_SEC = 24 * 3600
    # 序号33事务涉及的模块表
    TX_MODULES = ["sliding_index", "sink_output", "backfill_job", "window_config"]

    # ------------------------------------------------------------------
    # 指标版本与归档（历史结果需要保留指标版本）
    # ------------------------------------------------------------------

    def _ensure_version(self, row: dict[str, Any]) -> int:
        version = row.get("_version")
        if not isinstance(version, int) or version < 1:
            version = 1
            row["_version"] = version
        row.setdefault("_archived_versions", [])
        return version

    def _archive_version(
        self, row: dict[str, Any], reason: str, now: float,
    ) -> dict[str, Any]:
        """把当前定义快照写入归档版本。幂等：同一版本只归档一次。"""
        version = self._ensure_version(row)
        archives = row["_archived_versions"]
        for existing in archives:
            if existing.get("version") == version:
                return existing
        snapshot = {
            "version": version,
            "archived_at": now,
            "reason": reason,
            "definition": {field: row.get(field) for field in self.LIST_FIELDS},
        }
        archives.append(snapshot)
        return snapshot

    def _retention_due(self, row: dict[str, Any], now: float) -> bool:
        """判断保留时长是否到期（解析失败时按默认保留时长）。"""
        try:
            retention_sec = int(row.get("保留时长", self.DEFAULT_RETENTION_HOURS)) * 3600
        except (ValueError, TypeError):
            retention_sec = self.DEFAULT_RETENTION_HOURS * 3600
        ts = row.get("_computed_at", 0)
        return (
            isinstance(ts, (int, float))
            and ts > 0
            and now - ts > retention_sec
        )

    # ------------------------------------------------------------------
    # 序号33 · 读取指标引用
    # ------------------------------------------------------------------

    def find_metric_references(self, index_name: str) -> dict[str, Any]:
        """扫描指标的当前引用：窗口指标数据、窗口策略绑定、进行中补算。

        「当前窗口结果」指 sink_output 中该指标的、未被取代（_superseded）
        且未解除引用（_released）的窗口指标数据。
        """
        current_results: list[dict[str, Any]] = []
        for entry in store.rows("sink_output"):
            data = entry.get("data")
            is_metric_data = (
                (isinstance(data, dict) and data.get("index") == index_name)
                or str(entry.get("配置名称", "")).startswith(f"index_{index_name}_")
            )
            if not is_metric_data:
                continue
            if entry.get("_superseded") or entry.get("_released"):
                continue
            current_results.append({
                "sink_id": entry.get("id"),
                "window_key": entry.get("_window_key", ""),
                "group": data.get("group", "") if isinstance(data, dict) else "",
                "metric_version": entry.get("_metric_version"),
                "written_at": entry.get("written_at"),
            })

        window_policies: list[dict[str, Any]] = []
        for policy in store.rows("window_config"):
            if policy.get("status") == "已停用":
                continue
            bound = policy.get("_bound_metrics") or []
            defs = policy.get("window_definitions") or []
            metric_in_defs = any(
                index_name in str(d.get("metric", ""))
                for d in defs
                if isinstance(d, dict)
            )
            if index_name in bound or metric_in_defs:
                window_policies.append({
                    "policy_id": policy.get("id"),
                    "policy_name": policy.get("策略名称", ""),
                    "status": policy.get("status", ""),
                })

        backfill_jobs: list[dict[str, Any]] = []
        for job in store.rows("backfill_job"):
            if job.get("目标指标") == index_name and job.get("status") in ("待执行", "补数中"):
                backfill_jobs.append({
                    "job_id": job.get("id"),
                    "job_name": job.get("作业名称", ""),
                    "status": job.get("status", ""),
                })

        return {
            "current_results": current_results,
            "window_policies": window_policies,
            "backfill_jobs": backfill_jobs,
            "referenced": bool(current_results or window_policies),
        }

    # ------------------------------------------------------------------
    # 序号33 · 过期清理（读取引用 → 保留/停用 → 更新迟到补算清单）
    # ------------------------------------------------------------------

    def _ensure_late_backfill_entry(
        self, row: dict[str, Any], refs: dict[str, Any], now: float,
    ) -> dict[str, Any] | None:
        """把「窗口仍引用过期指标」的状态写入迟到补算清单。幂等：同指标同版本只写一条。"""
        name = row.get("指标名称", "")
        version = row.get("_version", 1)
        for job in store.rows("backfill_job"):
            if (
                job.get("目标指标") == name
                and job.get("_source") == "metric_retention"
                and job.get("_metric_version") == version
                and job.get("status") in ("待执行", "补数中")
            ):
                return None  # 已存在，保持幂等
        windows = [r["window_key"] for r in refs["current_results"] if r.get("window_key")]
        job = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows("backfill_job")), default=0
            ) + 1,
            "作业名称": f"迟到补算_{name}_v{version}",
            "目标指标": name,
            "补数范围": f"{windows[0]}~{windows[-1]}" if windows else "-",
            "并行度": 1,
            "起始位点": str(int(now)),
            "截止位点": str(int(now)),
            "进度比例": "0%",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "_source": "metric_retention",
            "_metric_version": version,
            "_blocked_windows": windows,
        }
        store.rows("backfill_job").append(job)
        return job

    def expire_old_indexes(
        self, run_key: str | None = None, now: float | None = None,
    ) -> dict[str, Any]:
        """清理过期指标定义：先读窗口引用，再决定停用或保留。

        - 被当前窗口结果/窗口策略引用的到期指标：保留定义、归档版本、
          把状态写入迟到补算清单（backfill_job），等待引用解除。
        - 引用已全部解除的到期指标：归档版本后停用为「已过期」。
        - 幂等：同一 run_key 重复执行直接返回上次结果；
          归档与补算清单按版本去重，重复执行不产生重复记录。
        - 事务：任一环节失败，恢复所有涉及模块的原记录。
        """
        now = now if now is not None else time.time()
        if run_key:
            replay = store.cleanup_run_replay(run_key)
            if replay is not None:
                result = dict(replay)
                result["idempotent_replay"] = True
                return result

        snapshot = store.snapshot_modules(self.TX_MODULES)
        try:
            with store._lock:
                expired: list[str] = []
                kept: list[dict[str, Any]] = []
                backfilled: list[str] = []
                for row in store.rows(self.MODULE):
                    if row.get("status") == "已过期":
                        continue
                    if not self._retention_due(row, now):
                        continue
                    name = row.get("指标名称", "")
                    self._ensure_version(row)

                    # 重算请求的延长保留尚未结束 → 保留到窗口发布完成
                    extended_until = row.get("_retention_extended_until", 0)
                    if isinstance(extended_until, (int, float)) and extended_until > now:
                        kept.append({
                            "index": name,
                            "reason": "retention_extended",
                            "extended_until": extended_until,
                        })
                        continue

                    refs = self.find_metric_references(name)
                    if refs["referenced"]:
                        # 窗口仍引用 → 保留定义 + 归档版本 + 写迟到补算清单
                        self._archive_version(row, "expire_blocked_keep_definition", now)
                        blocked_by = (
                            [p["policy_name"] for p in refs["window_policies"]]
                            + [r["window_key"] for r in refs["current_results"]]
                        )
                        row["_expire_blocked_by"] = blocked_by
                        row["_expire_blocked_at"] = now
                        kept.append({
                            "index": name,
                            "reason": "window_referenced",
                            "blocked_by": blocked_by,
                        })
                        job = self._ensure_late_backfill_entry(row, refs, now)
                        if job is not None:
                            backfilled.append(job["作业名称"])
                    else:
                        # 引用已解除 → 归档版本后停用
                        self._archive_version(row, "expired", now)
                        row["status"] = "已过期"
                        row["pending"] = False
                        row.pop("_expire_blocked_by", None)
                        expired.append(name)

                result = {
                    "ok": True,
                    "expired": expired,
                    "kept": kept,
                    "backfill_entries": backfilled,
                    "timestamp": now,
                    "run_key": run_key,
                }
                if run_key:
                    store.record_cleanup_run(run_key, result)
                return result
        except Exception:
            store.restore_modules(snapshot)  # 序号33事务失败恢复原记录
            raise

    # ------------------------------------------------------------------
    # 序号33 · 窗口重算（保留到期与重算同时到达的两个方案）
    # ------------------------------------------------------------------

    def recalculate_window(
        self,
        index_name: str,
        window_key: str,
        strategy: str = "auto",
        data_series: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """重算同一窗口：只能使用仍有效的指标定义。

        保留时长到期与窗口重算同时到达时的取舍：
        - ``extend``：延长保留直到窗口发布完成。定义继续可用、重算直接进行，
          代价是过期定义滞留、清理延迟。
        - ``archive``：复制指标版本到归档后立即清理。口径冻结、定义不滞留，
          代价是后续重算需引用归档版本。
        两种方案都会把该窗口的旧结果标记为已取代，当前可见结果只保留
        一个可解释口径。事务失败恢复原记录。
        """
        row = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == index_name:
                row = r
                break
        if row is None:
            return {"ok": False, "error": f"指标「{index_name}」不存在"}

        now = time.time()
        snapshot = store.snapshot_modules(self.TX_MODULES)
        try:
            with store._lock:
                if row.get("status") == "已过期":
                    return {
                        "ok": False,
                        "error": "指标已过期，重算同一窗口只能使用仍有效的指标定义",
                        "archived_versions": row.get("_archived_versions", []),
                    }
                self._ensure_version(row)
                due = self._retention_due(row, now)
                chosen = strategy
                if due and chosen == "auto":
                    chosen = "archive"  # 默认归档后清理，避免过期定义滞留
                if not due:
                    chosen = "none"

                extended_until = None
                if chosen == "extend":
                    # 方案A：延长保留直到窗口发布完成
                    extended_until = now + self.RETENTION_EXTEND_SEC
                    row["_retention_extended_until"] = extended_until
                elif chosen == "archive":
                    # 方案B：复制指标版本到归档后立即清理
                    self._archive_version(row, "recalc_archive_then_clean", now)
                    row["status"] = "已过期"
                    row["pending"] = False

                # 当前可见结果只保留一个可解释口径：旧版本结果标记为已取代
                superseded = 0
                for entry in store.rows("sink_output"):
                    data = entry.get("data")
                    if not (isinstance(data, dict) and data.get("index") == index_name):
                        continue
                    if window_key and entry.get("_window_key") != window_key:
                        continue
                    if not entry.get("_superseded"):
                        entry["_superseded"] = True
                        superseded += 1

                # 用仍有效的定义（或归档时冻结的口径）完成本次窗口发布
                compute = self.compute_and_output_to_sink(
                    index_name, data_series or [], window_key=window_key or None,
                )

                # 窗口发布完成：延长保留使命结束，引用随之解除
                row.pop("_retention_extended_until", None)
                return {
                    "ok": True,
                    "index": index_name,
                    "window_key": window_key,
                    "strategy": chosen,
                    "extended_until": extended_until,
                    "superseded": superseded,
                    "outputs_written": compute.get("outputs_written", 0),
                    "metric_version": row.get("_version", 1),
                    "index_status": row.get("status"),
                }
        except Exception:
            store.restore_modules(snapshot)  # 序号33事务失败恢复原记录
            raise

    # ------------------------------------------------------------------
    # 序号33 · 查询接口（并发查询只返回一版、幂等）
    # ------------------------------------------------------------------

    def _single_version_view(
        self, entries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """并发查询只保留一个版本：被取代的结果不进结果集。

        正常情况下只返回未取代（当前可解释口径）的记录；若某窗口的记录
        全部已被取代，则保留该窗口分组下版本最高的一条，保证历史结果
        仍能找到对应指标版本。
        """
        live = [e for e in entries if not e.get("_superseded")]
        if live:
            return live
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for entry in entries:
            data = entry.get("data") if isinstance(entry.get("data"), dict) else {}
            key = (str(entry.get("_window_key", "")), str(data.get("group", "")))
            current = latest.get(key)
            rank = (entry.get("_metric_version") or 0, entry.get("written_at") or 0)
            if current is None:
                latest[key] = entry
                continue
            current_rank = (
                current.get("_metric_version") or 0,
                current.get("written_at") or 0,
            )
            if rank > current_rank:
                latest[key] = entry
        return list(latest.values())

    def window_metric_data(
        self,
        index_name: str,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        sort: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> dict[str, Any]:
        """序号33查询接口：筛选/检索/排序/分页定位窗口指标数据。

        - 只返回当前可解释口径（单版本），过期指标的数据用归档版本解释；
        - 排序稳定且带确定性 tiebreaker，同参数并发/重复查询返回同一结果；
        - 读锁内取快照，写事务（清理/重算）不会查到中间态。
        """
        row = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == index_name:
                row = r
                break
        if row is None:
            return {"ok": False, "error": f"指标「{index_name}」不存在"}

        with store._lock:
            entries = []
            for entry in store.rows("sink_output"):
                data = entry.get("data")
                is_metric_data = (
                    (isinstance(data, dict) and data.get("index") == index_name)
                    or str(entry.get("配置名称", "")).startswith(f"index_{index_name}_")
                )
                if is_metric_data:
                    entries.append(dict(entry))
            current_version = row.get("_version", 1)
            archived = {
                a.get("version") for a in row.get("_archived_versions", [])
            }
            index_status = row.get("status", "")

        items = []
        for entry in self._single_version_view(entries):
            data = entry.get("data") if isinstance(entry.get("data"), dict) else {}
            version = entry.get("_metric_version", current_version)
            explainable = (
                (index_status != "已过期" and version == current_version)
                or version in archived
            )
            items.append({
                "window_key": entry.get("_window_key", ""),
                "group": data.get("group", ""),
                "value": data.get("value"),
                "metric_version": version,
                "index_status": index_status,
                "explainable": explainable,
                "archived_version": version in archived,
                "written_at": entry.get("written_at", 0),
            })

        # 筛选（保持当前条件：调用方把条件放在 query 中，跨页面可复现）
        for field, expected in (filters or {}).items():
            if field in ("window_key", "group", "metric_version") and expected != "":
                items = [
                    item for item in items
                    if str(expected).casefold() in str(item.get(field, "")).casefold()
                ]
            elif field == "explainable" and expected != "":
                want = str(expected).lower() in ("1", "true", "是")
                items = [item for item in items if item["explainable"] == want]
        if keyword:
            needle = keyword.casefold()
            items = [
                item for item in items
                if needle in str(item["window_key"]).casefold()
                or needle in str(item["group"]).casefold()
            ]

        # 稳定排序 + 确定性 tiebreaker：幂等，同参数查询返回同一结果
        def tiebreaker(item: dict[str, Any]) -> tuple[str, str, float]:
            return (
                str(item["window_key"]),
                str(item["group"]),
                float(item.get("written_at") or 0),
            )

        if sort == "written_at_asc":
            items.sort(key=lambda i: (float(i.get("written_at") or 0), *tiebreaker(i)[:2]))
        elif sort == "value_desc":
            items.sort(key=lambda i: (-(i.get("value") or 0), *tiebreaker(i)))
        elif sort == "value_asc":
            items.sort(key=lambda i: ((i.get("value") or 0), *tiebreaker(i)))
        elif sort == "window_key":
            items.sort(key=tiebreaker)
        else:  # 默认 written_at_desc
            items.sort(key=lambda i: (-float(i.get("written_at") or 0), *tiebreaker(i)[:2]))

        total = len(items)
        page = max(1, page)
        size = max(1, min(size, 200))
        start = (page - 1) * size
        return {
            "ok": True,
            "index": index_name,
            "index_status": index_status,
            "version": current_version,
            "items": items[start:start + size],
            "total": total,
            "page": page,
            "size": size,
            "single_version": True,
        }

    # ------------------------------------------------------------------
    # 序号33 · 指标页引用视图（异常提示后分析人员查看引用窗口）
    # ------------------------------------------------------------------

    def metric_reference_view(self, entry_id: int) -> dict[str, Any]:
        """指标页面的引用视图：引用窗口、归档版本、历史结果可解释性。"""
        row = store.find(self.MODULE, entry_id)
        if row is None:
            return {"ok": False, "error": f"{self.ENTITY} {entry_id} 不存在或已归档"}
        name = row.get("指标名称", "")
        refs = self.find_metric_references(name)
        archives = row.get("_archived_versions", [])
        archived_versions = {a.get("version") for a in archives}
        current_version = row.get("_version", 1)
        status = row.get("status", "")

        # 历史结果仍能找到对应指标版本：每条窗口指标数据的来源版本
        # 都能在当前定义（未过期时）或归档版本中找到
        history_explainable = True
        for entry in store.rows("sink_output"):
            data = entry.get("data")
            if not (isinstance(data, dict) and data.get("index") == name):
                continue
            version = entry.get("_metric_version", current_version)
            if status == "已过期" and version not in archived_versions:
                history_explainable = False
                break

        return {
            "ok": True,
            "id": entry_id,
            "index": name,
            "status": status,
            "version": current_version,
            "references": refs,
            "archived_versions": archives,
            "expire_blocked_by": row.get("_expire_blocked_by", []),
            "history_explainable": history_explainable,
        }

    # ------------------------------------------------------------------
    # 既有能力
    # ------------------------------------------------------------------

    def evaluate_index_condition(
        self, index_name: str, data: dict[str, Any],
    ) -> dict[str, Any]:
        """评估指标的过滤条件，判断数据是否满足计算条件。"""
        index_def = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == index_name:
                index_def = r
                break
        if index_def is None:
            return {"ok": False, "error": f"指标「{index_name}」不存在"}

        filter_cond = index_def.get("过滤条件", "")
        matched = True
        details = {}
        if filter_cond:
            parts = filter_cond.split(";")
            for cond in parts:
                cond = cond.strip()
                if not cond:
                    continue
                if "=" in cond:
                    k, v = cond.split("=", 1)
                    k, v = k.strip(), v.strip()
                    data_val = str(data.get(k, ""))
                    matched_cond = data_val == v
                    matched = matched and matched_cond
                    details[k] = {"expected": v, "actual": data_val, "match": matched_cond}
                elif ">" in cond:
                    k, v = cond.split(">", 1)
                    try:
                        matched = matched and float(data.get(k.strip(), 0)) > float(v.strip())
                    except (ValueError, TypeError):
                        matched = False

        return {
            "ok": True,
            "index": index_name,
            "matched": matched,
            "filter": filter_cond,
            "details": details,
        }

    def compute_and_output_to_sink(
        self,
        index_name: str,
        data_series: list[dict[str, Any]],
        window_key: str | None = None,
    ) -> dict[str, Any]:
        """执行指标计算并将结果输出到 sink_output。

        每条窗口指标数据都携带 ``_metric_version`` 来源版本和
        ``_window_key`` 窗口标识，供序号33的引用检查与单版本查询使用。
        """
        index_def = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == index_name:
                index_def = r
                break
        if index_def is None:
            return {"ok": False, "error": f"指标「{index_name}」不存在"}

        metric_version = self._ensure_version(index_def)
        group_dims = [
            d.strip()
            for d in index_def.get("分组维度", "").split(",")
            if d.strip()
        ]
        agg_func = index_def.get("聚合函数", "count").lower()

        # 分组聚合
        groups: dict[str, list[float]] = {}
        for item in data_series:
            payload = item.get("payload", item)
            gk_parts = [str(payload.get(d, "")) for d in group_dims]
            gk = "|".join(gk_parts) or "__all__"
            val = payload.get("value", 0)
            if not isinstance(val, (int, float)):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    val = 0.0
            groups.setdefault(gk, []).append(val)

        # 计算并写入 sink_output
        output_count = 0
        for gk, vals in groups.items():
            if agg_func == "count":
                result = float(len(vals))
            elif agg_func == "sum":
                result = sum(vals)
            elif agg_func == "avg":
                result = sum(vals) / len(vals) if vals else 0
            else:
                result = sum(vals)

            sink_entry = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("sink_output")), default=0
                ) + 1,
                "配置名称": f"index_{index_name}_{gk}",
                "输出类型": "index_result",
                "目标地址": f"index://{index_name}",
                "写入模式": "append",
                "分区策略": "round_robin",
                "格式转换": "json",
                "status": "已生效",
                "sink": f"index_{index_name}",
                "write_status": "success",
                "partition": f"pt_{gk}",
                "data": {"index": index_name, "group": gk, "value": result},
                "written_at": time.time(),
                "_metric_version": metric_version,
                "_window_key": window_key or f"w_{index_name}_{gk}_{int(time.time())}",
                "_superseded": False,
                "_released": False,
            }
            store.rows("sink_output").append(sink_entry)
            output_count += 1

        index_def["_computed_at"] = time.time()
        return {
            "ok": True,
            "index": index_name,
            "groups": len(groups),
            "outputs_written": output_count,
        }

    def release_window_results(
        self, index_name: str, window_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        """解除窗口结果对指标的引用（窗口发布完成/下线后调用）。

        过期指标只有在所有当前窗口结果解除引用后才能停用。
        """
        released = 0
        with store._lock:
            for entry in store.rows("sink_output"):
                data = entry.get("data")
                if not (isinstance(data, dict) and data.get("index") == index_name):
                    continue
                if window_keys and entry.get("_window_key") not in window_keys:
                    continue
                if not entry.get("_released"):
                    entry["_released"] = True
                    released += 1
        return {"ok": True, "index": index_name, "released": released}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active"] = sum(1 for r in rows if r.get("status") == "计算中")
        base["effective"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["expired"] = sum(1 for r in rows if r.get("status") == "已过期")
        base["expire_blocked"] = sum(1 for r in rows if r.get("_expire_blocked_by"))
        base["archived_versions"] = sum(
            len(r.get("_archived_versions", [])) for r in rows
        )
        return base
