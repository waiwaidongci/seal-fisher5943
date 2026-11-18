import {migration} from '../../api/research.js';
import {esc} from '../../utils/format.js';

const PHASE_LABELS={
  legacy:'迁前 · 旧浮点存储',
  dual:'在线回填 · 双写',
  switching:'原子切换中',
  integer:'已切换 · 整数存储',
};

// 全局缓存：各页面共享同一份迁移状态，避免重复请求
let cache=null,loading=null;
export async function loadMigrationStatus(force=false){
  if(cache && !force) return cache;
  if(!loading) loading=migration.status().then(s=>{cache=s;loading=null;return s;}).catch(e=>{loading=null;throw e;});
  return loading;
}
export function cachedMigrationStatus(){return cache;}
export function clearMigrationCache(){cache=null;}

export function phaseBadge(status){
  if(!status) return '';
  return `<span class="mig-phase ${esc(status.phase)}">${esc(PHASE_LABELS[status.phase]||status.phase)}</span>`;
}

// 来源录入页顶部：呈现迁移阶段、差异与当前读取依据
export function migrationBanner(status){
  if(!status) return '';
  const diffBlock=status.diffs&&status.diffs.length
    ? `<details class="mig-diffs" open><summary>校验差异 ${status.diffs.length} 处（已拒绝切换）</summary><pre>${esc(JSON.stringify(status.diffs,null,2))}</pre></details>`
    : '<small class="mig-ok">无校验差异</small>';
  const skipBlock=status.skipped&&status.skipped.length
    ? `<details class="mig-diffs"><summary>无法回填的非整数记录 ${status.skipped.length} 条</summary><pre>${esc(JSON.stringify(status.skipped,null,2))}</pre></details>`
    : '';
  return `<section class="mig-banner ${esc(status.phase)}">
    <div class="mig-banner-head">${phaseBadge(status)}
      <span class="mig-basis">当前读取依据：${esc(status.read_basis)}</span></div>
    <div class="mig-meta">
      <span>旧浮点行 ${status.legacy_rows??0}</span>
      <span>整数行 ${status.integer_rows??0}</span>
      <span>已回填 ${status.backfilled??0}</span>
      <span>游标 ID ${status.last_cursor_id??0}</span>
      <span>更新于 ${esc(status.updated_at||'—')}</span>
    </div>
    <p class="mig-detail">${esc(status.detail||'')}</p>
    ${diffBlock}${skipBlock}
  </section>`;
}
