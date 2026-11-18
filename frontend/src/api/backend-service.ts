import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  CleanupResult,
  EntryRow,
  MetricReferenceView,
  MetricVersionConfirmation,
  OverviewResult,
  PageResult,
  SyncMetricRefsResult,
  WindowMetricDataResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 序号33 · 指标过期清理与窗口引用检查 ----

export async function runMetricCleanup(runKey?: string): Promise<CleanupResult> {
  const response = await request('/api/sliding_index/cleanup/run', {
    method: 'POST',
    body: JSON.stringify({ run_key: runKey ?? null }),
  })
  if (!response.ok) throw new Error('过期清理执行失败')
  return await response.json()
}

export async function loadMetricReferences(id: number): Promise<MetricReferenceView> {
  const response = await request(`/api/sliding_index/${id}/references`)
  if (!response.ok) throw new Error('引用窗口读取失败')
  return await response.json()
}

export type WindowMetricQuery = {
  window_key?: string
  group?: string
  keyword?: string
  sort?: string
  page?: number
  size?: number
}

export async function loadWindowMetricData(
  id: number,
  params: WindowMetricQuery = {},
): Promise<WindowMetricDataResult> {
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      query.set(key, String(value))
    }
  }
  const response = await request(`/api/sliding_index/${id}/window_metrics?${query}`)
  if (!response.ok) throw new Error('窗口指标数据读取失败')
  return await response.json()
}

export async function recalculateWindow(
  id: number,
  windowKey: string,
  strategy: string,
): Promise<{ ok: boolean; message?: string; error?: string }> {
  const response = await request(`/api/sliding_index/${id}/recalculate`, {
    method: 'POST',
    body: JSON.stringify({ window_key: windowKey, strategy, data_series: [] }),
  })
  const payload = await response.json()
  if (!response.ok) {
    return { ok: false, error: payload?.detail ?? '重算失败' }
  }
  return { ok: true, ...payload }
}

export async function syncWindowMetricRefs(): Promise<SyncMetricRefsResult> {
  const response = await request('/api/window_config/sync_metric_refs', { method: 'POST' })
  if (!response.ok) throw new Error('同步过期指标引用失败')
  return await response.json()
}

export async function loadMetricVersionConfirmation(): Promise<MetricVersionConfirmation> {
  const response = await request('/api/window_config/metric_version_confirmation')
  if (!response.ok) throw new Error('指标版本确认读取失败')
  return await response.json()
}
