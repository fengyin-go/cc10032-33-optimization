"""序号33 · 指标过期清理与窗口引用检查测试。

覆盖：
  - 过期清理读取窗口引用：被引用的到期指标保留定义并写入迟到补算清单
  - 引用全部解除后过期指标才能停用
  - 历史结果保留指标版本（归档），过期指标有归档版本可解释
  - 重算同一窗口只能使用仍有效的指标定义
  - 保留到期与重算同时到达：extend 延长保留 / archive 归档后清理
  - 序号33查询接口：并发查询只返回一版、幂等、筛选排序分页确定
  - 序号33事务失败恢复原记录
  - 窗口配置模块读取过期指标清单并保留来源版本
"""
from __future__ import annotations

import threading
import time

import pytest

from app.services import service_for
from app.store import store


def _sliding():
    return service_for("sliding_index")


def _window():
    return service_for("window_config")


def _make_index(name: str, *, computed_at: float, retention_hours: str = "1",
                status: str = "已生效") -> dict:
    """登记一个保留时长已到期的测试指标。"""
    row = {
        "id": max((int(r.get("id", 0)) for r in store.rows("sliding_index")), default=0) + 1,
        "指标名称": name,
        "计算窗口": "10s",
        "聚合函数": "sum",
        "分组维度": "user_id",
        "过滤条件": "",
        "快照频率": "1s",
        "保留时长": retention_hours,
        "status": status,
        "pending": False,
        "abnormal": False,
        "_computed_at": computed_at,
        "_version": 1,
    }
    store.rows("sliding_index").append(row)
    return row


def _make_window_result(index_name: str, window_key: str, group: str = "g1",
                        value: float = 1.0, version: int = 1) -> dict:
    """写入一条窗口指标数据（模拟指标输出记录）。"""
    entry = {
        "id": max((int(r.get("id", 0)) for r in store.rows("sink_output")), default=0) + 1,
        "配置名称": f"index_{index_name}_{group}",
        "输出类型": "index_result",
        "status": "已生效",
        "data": {"index": index_name, "group": group, "value": value},
        "written_at": time.time(),
        "_metric_version": version,
        "_window_key": window_key,
        "_superseded": False,
        "_released": False,
    }
    store.rows("sink_output").append(entry)
    return entry


def _get_index(name: str) -> dict | None:
    for row in store.rows("sliding_index"):
        if row.get("指标名称") == name:
            return row
    return None


# ---------------------------------------------------------------------------
# 过期清理：读取指标引用
# ---------------------------------------------------------------------------

class TestExpireWithReferences:
    def test_expire_blocked_by_window_reference(self):
        """窗口结果仍引用的到期指标：保留定义、归档版本、写迟到补算清单。"""
        name = "idx33_block_ref"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_1")

        result = _sliding().expire_old_indexes()

        row = _get_index(name)
        assert row["status"] == "已生效", "被引用的指标不应被停用"
        assert name in [k["index"] for k in result["kept"]]
        assert row["_expire_blocked_by"], "应记录阻塞引用的窗口"
        # 历史结果保留指标版本
        assert any(a["version"] == 1 for a in row["_archived_versions"])
        # 状态写入迟到补算清单
        jobs = [
            j for j in store.rows("backfill_job")
            if j.get("目标指标") == name and j.get("_source") == "metric_retention"
        ]
        assert len(jobs) == 1
        assert jobs[0]["status"] == "待执行"
        assert jobs[0]["_metric_version"] == 1

    def test_expire_without_reference(self):
        """无引用的到期指标：归档版本后停用为已过期。"""
        name = "idx33_no_ref"
        _make_index(name, computed_at=time.time() - 7200)

        result = _sliding().expire_old_indexes()

        row = _get_index(name)
        assert row["status"] == "已过期"
        assert name in result["expired"]
        assert any(a["version"] == 1 for a in row["_archived_versions"])

    def test_expire_only_after_all_references_released(self):
        """过期指标只有在所有当前窗口结果解除引用后才能停用。"""
        name = "idx33_release"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_a")
        _make_window_result(name, "w_b")

        _sliding().expire_old_indexes()
        assert _get_index(name)["status"] == "已生效", "仍有引用时不能停用"

        # 只解除一个窗口的引用 → 仍不能停用
        _sliding().release_window_results(name, window_keys=["w_a"])
        _sliding().expire_old_indexes()
        assert _get_index(name)["status"] == "已生效"

        # 全部解除 → 停用
        _sliding().release_window_results(name)
        _sliding().expire_old_indexes()
        assert _get_index(name)["status"] == "已过期"

    def test_expire_idempotent_with_run_key(self):
        """同一 run_key 重复清理返回同一结果，归档与补算清单不重复。"""
        name = "idx33_idem"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_1")

        first = _sliding().expire_old_indexes(run_key="run-33-idem")
        second = _sliding().expire_old_indexes(run_key="run-33-idem")

        assert second["idempotent_replay"] is True
        assert first["kept"] == second["kept"]
        row = _get_index(name)
        assert len(row["_archived_versions"]) == 1, "归档版本不重复"
        jobs = [
            j for j in store.rows("backfill_job")
            if j.get("目标指标") == name and j.get("_source") == "metric_retention"
        ]
        assert len(jobs) == 1, "迟到补算清单不重复"

    def test_expire_repeated_without_key_no_duplicate_side_effects(self):
        """不传 run_key 重复清理：归档与补算清单仍按版本去重。"""
        name = "idx33_repeat"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_1")

        _sliding().expire_old_indexes()
        _sliding().expire_old_indexes()

        row = _get_index(name)
        assert len(row["_archived_versions"]) == 1
        jobs = [
            j for j in store.rows("backfill_job")
            if j.get("目标指标") == name and j.get("_source") == "metric_retention"
        ]
        assert len(jobs) == 1


# ---------------------------------------------------------------------------
# 窗口重算：只能使用仍有效的指标定义
# ---------------------------------------------------------------------------

class TestRecalculateWindow:
    def test_recalc_rejected_for_expired_index(self):
        """已过期指标不能重算同一窗口。"""
        name = "idx33_recalc_expired"
        _make_index(name, computed_at=time.time() - 7200, status="已过期")

        result = _sliding().recalculate_window(name, "w_1")

        assert result["ok"] is False
        assert "仍有效" in result["error"]

    def test_recalc_extend_retention_until_publish(self):
        """方案A：保留到期与重算同时到达 → 延长保留直到窗口发布完成。"""
        name = "idx33_extend"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_ext")

        result = _sliding().recalculate_window(
            name, "w_ext", strategy="extend",
            data_series=[{"payload": {"user_id": "u1", "value": 5.0}}],
        )

        assert result["ok"] is True
        assert result["strategy"] == "extend"
        assert result["extended_until"] is not None
        row = _get_index(name)
        assert row["status"] == "已生效", "延长保留期间定义继续可用"
        # 窗口发布完成后延期解除
        assert "_retention_extended_until" not in row

    def test_recalc_archive_then_clean(self):
        """方案B：复制指标版本到归档后立即清理，历史结果仍可解释。"""
        name = "idx33_archive"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_arc", version=1)

        result = _sliding().recalculate_window(
            name, "w_arc", strategy="archive",
            data_series=[{"payload": {"user_id": "u1", "value": 7.0}}],
        )

        assert result["ok"] is True
        assert result["strategy"] == "archive"
        row = _get_index(name)
        assert row["status"] == "已过期", "归档后立即清理"
        assert any(a["version"] == 1 for a in row["_archived_versions"])
        # 过期指标有归档版本：历史结果仍能找到对应指标版本
        view = _sliding().metric_reference_view(row["id"])
        assert view["history_explainable"] is True

    def test_recalc_single_visible_version(self):
        """重算后当前可见结果只保留一个可解释口径。"""
        name = "idx33_single"
        _make_index(name, computed_at=time.time())  # 未到期
        _make_window_result(name, "w_sv", value=1.0)

        _sliding().recalculate_window(
            name, "w_sv",
            data_series=[{"payload": {"user_id": "u1", "value": 9.0}}],
        )

        data = _sliding().window_metric_data(name)
        assert data["ok"] is True
        w_items = [i for i in data["items"] if i["window_key"] == "w_sv"]
        assert len(w_items) == 1, "同一窗口只保留一个可解释口径"
        assert w_items[0]["value"] == 9.0


# ---------------------------------------------------------------------------
# 序号33查询接口：并发查询只返回一版、幂等
# ---------------------------------------------------------------------------

class TestWindowMetricDataQuery:
    def test_query_returns_single_version(self):
        """被取代的历史版本不进结果集，并发查询只保留一个版本。"""
        name = "idx33_query_ver"
        _make_index(name, computed_at=time.time())
        _make_window_result(name, "w_q", value=1.0, version=1)
        _make_window_result(name, "w_q", value=2.0, version=2)
        # 模拟重算：v1 被取代
        for entry in store.rows("sink_output"):
            data = entry.get("data")
            if (
                isinstance(data, dict) and data.get("index") == name
                and entry.get("_metric_version") == 1
            ):
                entry["_superseded"] = True

        result = _sliding().window_metric_data(name)
        values = [i["value"] for i in result["items"] if i["window_key"] == "w_q"]
        assert values == [2.0], "只返回当前版本"
        assert result["single_version"] is True

    def test_query_idempotent_same_params_same_result(self):
        """同参数重复查询返回同一结果（幂等）。"""
        name = "idx33_query_idem"
        _make_index(name, computed_at=time.time())
        for i in range(5):
            _make_window_result(name, f"w_{i}", group=f"g{i}", value=float(i))

        params = dict(sort="value_desc", page=1, size=3)
        first = _sliding().window_metric_data(name, **params)
        second = _sliding().window_metric_data(name, **params)
        assert first["items"] == second["items"]
        assert first["total"] == 5
        assert len(first["items"]) == 3

    def test_query_filter_sort_pagination(self):
        """筛选、检索、排序、分页保持条件定位窗口指标数据。"""
        name = "idx33_query_fsp"
        _make_index(name, computed_at=time.time())
        _make_window_result(name, "w_keep_1", group="ga", value=3.0)
        _make_window_result(name, "w_keep_2", group="gb", value=1.0)
        _make_window_result(name, "w_other", group="ga", value=2.0)

        filtered = _sliding().window_metric_data(name, filters={"group": "ga"})
        assert all(i["group"] == "ga" for i in filtered["items"])
        assert filtered["total"] == 2

        searched = _sliding().window_metric_data(name, keyword="w_keep")
        assert searched["total"] == 2

        sorted_desc = _sliding().window_metric_data(name, sort="value_desc")
        values = [i["value"] for i in sorted_desc["items"]]
        assert values == sorted(values, reverse=True)

        page2 = _sliding().window_metric_data(name, sort="value_asc", page=2, size=2)
        assert page2["page"] == 2
        assert len(page2["items"]) == 1

    def test_expired_index_data_explainable_via_archive(self):
        """过期指标的窗口数据通过归档版本保持可解释。"""
        name = "idx33_query_exp"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_exp", version=1)
        service = _sliding()
        service.expire_old_indexes()  # 有引用 → 保留定义 + 归档版本
        service.release_window_results(name)  # 解除全部引用
        service.expire_old_indexes()  # 引用已解除 → 停用
        assert _get_index(name)["status"] == "已过期"

        data = service.window_metric_data(name)
        item = [i for i in data["items"] if i["window_key"] == "w_exp"][0]
        assert item["explainable"] is True
        assert item["archived_version"] is True

    def test_concurrent_cleanup_and_query_single_version(self):
        """清理与查询并发：查询结果始终一致且只保留一个版本。"""
        name = "idx33_concurrent"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_c", value=1.0)

        errors: list[Exception] = []
        totals: list[int] = []

        def run_cleanup():
            try:
                for _ in range(10):
                    _sliding().expire_old_indexes(run_key="run-33-concurrent")
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        def run_query():
            try:
                for _ in range(10):
                    data = _sliding().window_metric_data(name)
                    totals.append(data["total"])
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=run_cleanup) for _ in range(3)]
        threads += [threading.Thread(target=run_query) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(set(totals)) == 1, "并发查询只保留一个版本，结果集大小一致"
        row = _get_index(name)
        assert len(row["_archived_versions"]) == 1, "并发清理不重复归档"


# ---------------------------------------------------------------------------
# 序号33事务失败恢复原记录
# ---------------------------------------------------------------------------

class TestTransactionRollback:
    def test_cleanup_failure_restores_records(self, monkeypatch):
        """清理中途失败：指标状态、归档、补算清单全部恢复原记录。"""
        name = "idx33_tx_fail"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_tx")
        before_status = _get_index(name)["status"]
        before_jobs = len(store.rows("backfill_job"))

        service = _sliding()

        def boom(row, refs, now):
            raise RuntimeError("模拟写清单失败")

        monkeypatch.setattr(service, "_ensure_late_backfill_entry", boom)
        with pytest.raises(RuntimeError):
            service.expire_old_indexes()

        row = _get_index(name)
        assert row["status"] == before_status, "事务失败恢复原记录"
        assert "_expire_blocked_by" not in row
        assert not row.get("_archived_versions")
        assert len(store.rows("backfill_job")) == before_jobs

    def test_recalc_failure_restores_records(self, monkeypatch):
        """重算中途失败：指标状态与窗口结果恢复原记录。"""
        name = "idx33_tx_recalc"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_txr")

        service = _sliding()

        def boom(index_name, data_series, window_key=None):
            raise RuntimeError("模拟重算失败")

        monkeypatch.setattr(service, "compute_and_output_to_sink", boom)
        with pytest.raises(RuntimeError):
            service.recalculate_window(name, "w_txr", strategy="archive")

        # 快照恢复会替换表对象，重新获取记录再断言
        row = _get_index(name)
        assert row["status"] == "已生效", "失败回滚，不应被清理"
        assert not row.get("_archived_versions"), "失败回滚，不应留下归档"
        restored_entry = next(
            e for e in store.rows("sink_output")
            if isinstance(e.get("data"), dict) and e["data"].get("index") == name
        )
        assert restored_entry["_superseded"] is False, "失败回滚，旧结果不应被取代"


# ---------------------------------------------------------------------------
# 窗口配置模块：读取过期指标清单
# ---------------------------------------------------------------------------

class TestWindowConfigSync:
    def test_sync_keeps_referenced_definition(self):
        """窗口仍引用时保留定义、写迟到补算清单、保留来源版本。"""
        name = "idx33_sync"
        _make_index(name, computed_at=time.time() - 7200, status="已过期")
        entry = _make_window_result(name, "w_sync")
        del entry["_metric_version"]  # 模拟缺少来源版本的历史数据

        result = _window().sync_expired_metric_refs()

        synced = [s for s in result["synced"] if s["index"] == name]
        assert synced, "窗口仍引用的过期指标应被同步"
        assert synced[0]["restored"] is True, "保留定义：误标过期的恢复"
        row = _get_index(name)
        assert row["status"] == "已生效"
        # 窗口指标数据补上来源版本
        assert entry["_metric_version"] == row["_version"]
        # 迟到补算清单有记录
        jobs = [
            j for j in store.rows("backfill_job")
            if j.get("目标指标") == name and j.get("_source") == "metric_retention"
        ]
        assert len(jobs) == 1

    def test_sync_skips_unreferenced(self):
        """无引用的过期指标不在窗口同步范围内。"""
        name = "idx33_sync_skip"
        _make_index(name, computed_at=time.time() - 7200, status="已过期")

        result = _window().sync_expired_metric_refs()

        assert name not in [s["index"] for s in result["synced"]]
        assert _get_index(name)["status"] == "已过期"

    def test_window_page_version_confirmation(self):
        """窗口页面确认：历史结果仍能找到对应指标版本。"""
        name = "idx33_confirm"
        _make_index(name, computed_at=time.time() - 7200)
        _make_window_result(name, "w_cf", version=1)
        service = _sliding()
        service.expire_old_indexes()  # 有引用 → 保留 + 归档
        service.release_window_results(name)
        service.expire_old_indexes()  # 引用解除 → 停用

        view = _window().metric_version_confirmation()

        conf = [c for c in view["confirmations"] if c["index"] == name]
        assert conf, "窗口页面应能定位该指标的历史结果"
        assert conf[0]["all_explainable"] is True
        assert conf[0]["index_status"] == "已过期"
        assert 1 in conf[0]["archived_versions"], "过期指标 有归档版本"
