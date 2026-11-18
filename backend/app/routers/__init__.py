"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

metric_retention 的固定路径必须先于通用资源路由注册，
避免被 /api/{module}/{entry_id} 的动态段捕获。
"""
from __future__ import annotations

from app.routers import metric_retention, resource

ROUTERS = [metric_retention, resource]
