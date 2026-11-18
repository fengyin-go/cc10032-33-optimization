/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

/** 序号33 · 指标过期清理结果 */
export type CleanupResult = {
  ok: boolean
  expired: string[]
  kept: { index: string; reason: string; blocked_by?: string[] }[]
  backfill_entries: string[]
  timestamp: number
  run_key?: string | null
  idempotent_replay?: boolean
}

/** 序号33 · 指标引用视图（指标页面查看引用窗口） */
export type MetricReferenceView = {
  ok: boolean
  id: number
  index: string
  status: string
  version: number
  references: {
    current_results: { sink_id: number; window_key: string; group: string; metric_version: number | null }[]
    window_policies: { policy_id: number; policy_name: string; status: string }[]
    backfill_jobs: { job_id: number; job_name: string; status: string }[]
    referenced: boolean
  }
  archived_versions: { version: number; archived_at: number; reason: string }[]
  expire_blocked_by: string[]
  history_explainable: boolean
}

/** 序号33 · 窗口指标数据行（单版本、可解释口径） */
export type WindowMetricItem = {
  window_key: string
  group: string
  value: number | null
  metric_version: number
  index_status: string
  explainable: boolean
  archived_version: boolean
  written_at: number
}

export type WindowMetricDataResult = {
  ok: boolean
  index: string
  index_status: string
  version: number
  items: WindowMetricItem[]
  total: number
  page: number
  size: number
  single_version: boolean
}

/** 序号33 · 窗口配置同步过期指标引用结果 */
export type SyncMetricRefsResult = {
  ok: boolean
  synced: {
    index: string
    restored: boolean
    version_stamped: number
    backfill_job: string | null
    references: number
  }[]
  timestamp: number
}

/** 序号33 · 窗口页面版本确认（历史结果仍能找到对应指标版本） */
export type MetricVersionConfirmation = {
  ok: boolean
  confirmations: {
    index: string
    index_status: string
    current_version: number
    archived_versions: number[]
    results: {
      window_key: string
      group: string
      metric_version: number
      superseded: boolean
      version_found: boolean
    }[]
    all_explainable: boolean
  }[]
}
