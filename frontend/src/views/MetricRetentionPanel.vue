<template>
  <!-- 序号33 · 指标过期清理与窗口引用检查 -->
  <section v-if="moduleKey === 'sliding_index'" class="retention-panel" data-testid="retention-panel">
    <header class="retention-head">
      <h3>序号33 · 指标保留与窗口引用</h3>
      <div class="page-actions">
        <button class="btn primary" data-testid="run-cleanup" type="button" @click="runCleanup">
          执行过期清理
        </button>
        <button class="btn" type="button" @click="emitChanged">刷新列表</button>
      </div>
    </header>

    <p v-if="cleanupSummary" class="retention-summary" data-testid="cleanup-summary">{{ cleanupSummary }}</p>
    <p v-if="blockedHints.length" class="error-text" data-testid="retention-alert">
      序号33异常提示：以下到期指标仍被窗口引用，定义已保留并写入迟到补算清单：{{ blockedHints.join('、') }}
    </p>

    <div class="retention-grid">
      <!-- 指标引用视图 -->
      <div class="retention-card">
        <h4>指标引用窗口</h4>
        <div class="retention-form">
          <label>
            <span>选择指标</span>
            <select v-model="selectedId" data-testid="select-index">
              <option v-for="row in indexRows" :key="row.id" :value="Number(row.id)">
                {{ row['指标名称'] }}（{{ row.status }}）
              </option>
            </select>
          </label>
          <button class="btn" data-testid="load-references" type="button" @click="loadReferences">
            查看引用窗口
          </button>
        </div>

        <div v-if="refView" class="ref-view" data-testid="reference-view">
          <p>
            当前版本 v{{ refView.version }} · 状态 {{ refView.status }} ·
            历史结果可解释：<strong :class="{ 'error-text': !refView.history_explainable }">
              {{ refView.history_explainable ? '是' : '否' }}
            </strong>
          </p>
          <template v-if="refView.references.window_policies.length">
            <h5>引用窗口策略</h5>
            <ul>
              <li v-for="policy in refView.references.window_policies" :key="policy.policy_id">
                {{ policy.policy_name }}（{{ policy.status }}）
                <button class="link" type="button" @click="gotoWindowConfig(policy.policy_name)">
                  在窗口配置中定位
                </button>
              </li>
            </ul>
          </template>
          <p v-if="refView.references.current_results.length">
            当前窗口结果 {{ refView.references.current_results.length }} 条仍引用该指标
          </p>
          <p v-if="refView.references.backfill_jobs.length">
            迟到补算清单：
            <span v-for="job in refView.references.backfill_jobs" :key="job.job_id" class="legend-item">
              {{ job.job_name }}（{{ job.status }}）
            </span>
          </p>
          <template v-if="refView.archived_versions.length">
            <h5>归档版本</h5>
            <ul>
              <li v-for="ver in refView.archived_versions" :key="ver.version">
                v{{ ver.version }} · {{ ver.reason }} · {{ formatTs(ver.archived_at) }}
              </li>
            </ul>
          </template>
          <p v-if="refView.expire_blocked_by.length" class="error-text">
            过期被阻塞，引用方：{{ refView.expire_blocked_by.join('、') }}
          </p>
        </div>
      </div>

      <!-- 序号33详情：窗口指标数据 -->
      <div class="retention-card">
        <h4>窗口指标数据（序号33详情）</h4>
        <form class="retention-form" @submit.prevent="loadWindowData(1)">
          <label>
            <span>窗口标识</span>
            <input v-model="wmFilters.window_key" data-testid="wm-filter-window" placeholder="按窗口标识筛选" />
          </label>
          <label>
            <span>分组</span>
            <input v-model="wmFilters.group" data-testid="wm-filter-group" placeholder="按分组筛选" />
          </label>
          <label>
            <span>检索</span>
            <input v-model="wmKeyword" data-testid="wm-keyword" placeholder="窗口/分组关键字" />
          </label>
          <label>
            <span>排序</span>
            <select v-model="wmSort" data-testid="wm-sort">
              <option value="written_at_desc">写入时间↓</option>
              <option value="written_at_asc">写入时间↑</option>
              <option value="value_desc">数值↓</option>
              <option value="value_asc">数值↑</option>
              <option value="window_key">窗口标识</option>
            </select>
          </label>
          <button class="btn" data-testid="wm-query" type="submit">查询</button>
        </form>

        <table class="data-table" data-testid="wm-table">
          <thead>
            <tr>
              <th>窗口标识</th><th>分组</th><th>数值</th><th>指标版本</th><th>可解释</th><th>写入时间</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in wmData?.items ?? []" :key="`${item.window_key}-${item.group}`">
              <td>{{ item.window_key || '—' }}</td>
              <td>{{ item.group }}</td>
              <td>{{ item.value }}</td>
              <td>v{{ item.metric_version }}<span v-if="item.archived_version">（归档）</span></td>
              <td :class="{ 'error-text': !item.explainable }">{{ item.explainable ? '可解释' : '口径缺失' }}</td>
              <td>{{ formatTs(item.written_at) }}</td>
              <td>
                <button class="link" type="button" @click="recalc(item.window_key, 'extend')">延期重算</button>
                <button class="link" type="button" @click="recalc(item.window_key, 'archive')">归档重算</button>
              </td>
            </tr>
            <tr v-if="!wmData?.items?.length">
              <td colspan="7" class="empty-state">选择指标并查询后展示窗口指标数据</td>
            </tr>
          </tbody>
        </table>

        <footer class="page-foot">
          <span>共 {{ wmData?.total ?? 0 }} 条 · 第 {{ wmData?.page ?? 1 }} 页（单版本口径）</span>
          <span class="row-actions">
            <button class="link" type="button" :disabled="wmPage <= 1" data-testid="wm-prev" @click="loadWindowData(wmPage - 1)">上一页</button>
            <button
              class="link" type="button" data-testid="wm-next"
              :disabled="!wmData || wmPage * wmData.size >= wmData.total"
              @click="loadWindowData(wmPage + 1)"
            >下一页</button>
          </span>
        </footer>
        <p v-if="recalcMessage" class="retention-summary">{{ recalcMessage }}</p>
      </div>
    </div>
  </section>

  <!-- 窗口配置页：同步过期指标引用 + 历史结果版本确认 -->
  <section v-else-if="moduleKey === 'window_config'" class="retention-panel" data-testid="retention-panel">
    <header class="retention-head">
      <h3>序号33 · 过期指标引用同步</h3>
      <div class="page-actions">
        <button class="btn primary" data-testid="sync-refs" type="button" @click="syncRefs">
          同步过期指标引用
        </button>
        <button class="btn" data-testid="load-confirmations" type="button" @click="loadConfirmations">
          确认历史结果指标版本
        </button>
      </div>
    </header>

    <p v-if="syncSummary" class="retention-summary" data-testid="sync-summary">{{ syncSummary }}</p>

    <table v-if="confirmations.length" class="data-table" data-testid="confirmation-table">
      <thead>
        <tr>
          <th>指标名称</th><th>指标状态</th><th>当前版本</th><th>归档版本</th>
          <th>窗口结果数</th><th>历史结果可解释</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="conf in confirmations" :key="conf.index">
          <td>{{ conf.index }}</td>
          <td>{{ conf.index_status }}</td>
          <td>v{{ conf.current_version }}</td>
          <td>{{ conf.archived_versions.length ? conf.archived_versions.map((v) => `v${v}`).join('、') : '—' }}</td>
          <td>{{ conf.results.length }}</td>
          <td :class="{ 'error-text': !conf.all_explainable }">
            {{ conf.all_explainable ? '全部可解释' : '存在口径缺失' }}
          </td>
        </tr>
      </tbody>
    </table>
    <p v-else-if="confirmationsLoaded" class="retention-summary">暂无窗口指标数据需要确认</p>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'

import {
  loadMetricReferences,
  loadMetricVersionConfirmation,
  loadWindowMetricData,
  recalculateWindow,
  runMetricCleanup,
  syncWindowMetricRefs,
} from '@/api/backend-service'
import type {
  EntryRow,
  MetricReferenceView,
  MetricVersionConfirmation,
  WindowMetricDataResult,
} from '@/data/types'

const props = defineProps<{ moduleKey: string; rows: EntryRow[] }>()
const emit = defineEmits<{ changed: [] }>()
const router = useRouter()

const indexRows = computed(() => props.rows)
const selectedId = ref<number>(0)
const refView = ref<MetricReferenceView | null>(null)
const cleanupSummary = ref('')
const blockedHints = ref<string[]>([])
const recalcMessage = ref('')

const wmFilters = ref({ window_key: '', group: '' })
const wmKeyword = ref('')
const wmSort = ref('written_at_desc')
const wmPage = ref(1)
const wmData = ref<WindowMetricDataResult | null>(null)

const syncSummary = ref('')
const confirmations = ref<MetricVersionConfirmation['confirmations']>([])
const confirmationsLoaded = ref(false)

function formatTs(ts: number): string {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString()
}

function emitChanged() {
  emit('changed') // 序号33另一个业务面刷新列表
}

async function runCleanup() {
  cleanupSummary.value = ''
  blockedHints.value = []
  try {
    // run_key 按分钟生成：同一分钟内的重复点击保持幂等
    const runKey = `ui-cleanup-${Math.floor(Date.now() / 60000)}`
    const result = await runMetricCleanup(runKey)
    const kept = result.kept.filter((k) => k.reason === 'window_referenced')
    blockedHints.value = kept.map((k) => k.index)
    cleanupSummary.value =
      `清理完成：停用 ${result.expired.length} 个（${result.expired.join('、') || '无'}），` +
      `保留 ${result.kept.length} 个，写入迟到补算清单 ${result.backfill_entries.length} 条` +
      (result.idempotent_replay ? '（幂等重放，未重复执行）' : '')
    emitChanged()
  } catch (error) {
    cleanupSummary.value = error instanceof Error ? error.message : '清理失败'
  }
}

async function loadReferences() {
  if (!selectedId.value) return
  refView.value = await loadMetricReferences(selectedId.value)
}

async function loadWindowData(page: number) {
  if (!selectedId.value) return
  wmPage.value = page
  // 检索、过滤和排序保持当前条件，分页翻页定位
  wmData.value = await loadWindowMetricData(selectedId.value, {
    window_key: wmFilters.value.window_key,
    group: wmFilters.value.group,
    keyword: wmKeyword.value,
    sort: wmSort.value,
    page,
    size: 5,
  })
}

async function recalc(windowKey: string, strategy: 'extend' | 'archive') {
  if (!selectedId.value) return
  recalcMessage.value = ''
  const result = await recalculateWindow(selectedId.value, windowKey, strategy)
  if (!result.ok) {
    recalcMessage.value = `重算被拒绝：${result.error ?? '未知原因'}`
    return
  }
  recalcMessage.value =
    strategy === 'extend'
      ? '已延长保留直到窗口发布完成，重算完成'
      : '已复制指标版本到归档后立即清理，重算完成'
  await loadWindowData(wmPage.value)
  emitChanged()
}

function gotoWindowConfig(policyName: string) {
  // 序号33跨页面定位同一筛选结果：筛选条件随 query 传递到窗口配置页
  void router.push({ path: '/window_config', query: { 策略名称: policyName } })
}

async function syncRefs() {
  syncSummary.value = ''
  try {
    const result = await syncWindowMetricRefs()
    const restored = result.synced.filter((s) => s.restored).map((s) => s.index)
    syncSummary.value =
      `同步完成：${result.synced.length} 个指标窗口仍引用，定义已保留` +
      (restored.length ? `（恢复：${restored.join('、')}）` : '') +
      '，状态已写入迟到补算清单'
    emitChanged()
  } catch (error) {
    syncSummary.value = error instanceof Error ? error.message : '同步失败'
  }
}

async function loadConfirmations() {
  const result = await loadMetricVersionConfirmation()
  confirmations.value = result.confirmations
  confirmationsLoaded.value = true
}
</script>

<style scoped>
.retention-panel {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 12px;
}
.retention-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}
.retention-head h3 { margin: 0; font-size: 14px; }
.retention-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
@media (max-width: 1100px) {
  .retention-grid { grid-template-columns: 1fr; }
}
.retention-card {
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px;
}
.retention-card h4 { margin: 0 0 8px; font-size: 13px; }
.retention-card h5 { margin: 8px 0 4px; font-size: 12px; color: var(--muted); }
.retention-form {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: flex-end;
  margin-bottom: 8px;
}
.retention-form label { display: flex; flex-direction: column; gap: 2px; font-size: 12px; color: var(--muted); }
.retention-form input,
.retention-form select {
  padding: 5px 8px;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 13px;
}
.retention-summary { font-size: 13px; color: var(--muted); }
.ref-view { font-size: 13px; }
.ref-view ul { margin: 4px 0; padding-left: 18px; }
.legend-item { margin-right: 8px; }
</style>
