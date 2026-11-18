"""序号33专用接口：指标过期清理、窗口引用检查与窗口指标数据查询。

这些固定路径必须注册在通用资源路由 ``/api/{module}/{entry_id}`` 之前，
否则 ``metric_version_confirmation`` 等字面量会被动态段捕获并因 int
校验失败返回 422。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api", tags=["序号33指标保留"])


class CleanupPayload(BaseModel):
    run_key: str | None = None


class RecalculatePayload(BaseModel):
    window_key: str = ""
    strategy: str = "auto"  # auto / extend / archive
    data_series: list[dict[str, Any]] = Field(default_factory=list)


def _service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def _metric_name(service: Any, entry_id: int) -> str:
    row = service.get_entry(entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"指标计算 {entry_id} 不存在或已归档")
    return str(row.get("指标名称", ""))


@router.post("/sliding_index/cleanup/run")
def run_metric_cleanup(payload: CleanupPayload | None = None) -> dict[str, Any]:
    """执行过期清理：读取指标引用、清理过期定义并更新迟到补算清单。

    传入相同 run_key 时保持幂等，直接返回首次执行的结果。
    """
    service = _service("sliding_index")
    run_key = payload.run_key if payload else None
    return service.expire_old_indexes(run_key=run_key)


@router.get("/sliding_index/{entry_id}/references")
def metric_references(entry_id: int) -> dict[str, Any]:
    """指标页引用视图：引用窗口、归档版本、历史结果可解释性。"""
    service = _service("sliding_index")
    result = service.metric_reference_view(entry_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error"))
    return result


@router.get("/sliding_index/{entry_id}/window_metrics")
def window_metric_data(
    entry_id: int,
    window_key: str | None = Query(default=None),
    group: str | None = Query(default=None),
    explainable: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    sort: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    """序号33查询接口：筛选/检索/排序/分页定位窗口指标数据。

    并发查询只返回一版（当前可解释口径），同参数查询返回同一结果。
    """
    service = _service("sliding_index")
    name = _metric_name(service, entry_id)
    filters = {
        key: value
        for key, value in {
            "window_key": window_key,
            "group": group,
            "explainable": explainable,
        }.items()
        if value
    }
    return service.window_metric_data(
        name, filters=filters, keyword=keyword, sort=sort, page=page, size=size,
    )


@router.post("/sliding_index/{entry_id}/recalculate")
def recalculate_window(entry_id: int, payload: RecalculatePayload) -> dict[str, Any]:
    """重算同一窗口：只能使用仍有效的指标定义。

    保留到期与重算同时到达时按 strategy 取舍：extend 延长保留直到
    窗口发布完成；archive 复制指标版本到归档后立即清理。
    """
    service = _service("sliding_index")
    name = _metric_name(service, entry_id)
    result = service.recalculate_window(
        name,
        payload.window_key,
        strategy=payload.strategy,
        data_series=payload.data_series,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("error"))
    return result


@router.post("/sliding_index/{entry_id}/release")
def release_window_results(entry_id: int, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """解除窗口结果对指标的引用，过期指标随后才能停用。"""
    service = _service("sliding_index")
    name = _metric_name(service, entry_id)
    window_keys = (payload or {}).get("window_keys")
    return service.release_window_results(name, window_keys=window_keys)


@router.post("/window_config/sync_metric_refs")
def sync_metric_refs() -> dict[str, Any]:
    """窗口配置读取过期指标清单：窗口仍引用时保留定义并写迟到补算清单。"""
    service = _service("window_config")
    return service.sync_expired_metric_refs()


@router.get("/window_config/metric_version_confirmation")
def metric_version_confirmation() -> dict[str, Any]:
    """窗口页面确认：历史结果仍能找到对应指标版本。"""
    service = _service("window_config")
    return service.metric_version_confirmation()
