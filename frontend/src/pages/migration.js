import {migration} from '../api/research.js';
import {esc} from '../utils/format.js';
import {loadMigrationStatus,clearMigrationCache,migrationBanner,phaseBadge} from '../components/common/migration-banner.js';
import {table} from '../components/common/table.js';

const btn=(action,label,kind='')=>`<button class="mig-action ${kind}" data-mig="${action}">${label}</button>`;

function actions(s,role){
  if(role!=='admin') return '<p class="method-note">仅负责人（admin）可执行迁移动作；当前为只读视图。</p>';
  const p=s.phase;
  const groups=[];
  if(p==='legacy') groups.push(btn('start','① 启动在线回填（进入双写）'));
  if(p==='dual'||p==='switching'){
    groups.push(`<div class="mig-btn-group">${btn('backfill','② 继续回填一批（500 行）')}${btn('verify','③ 全量校验')}</div>`);
  }
  if(p==='dual') groups.push(btn('beginSwitch','④ 发起原子切换（暂停新研究冻结）','warn'));
  if(p==='switching') groups.push(`<div class="mig-btn-group">${btn('completeSwitch','⑤ 校验并完成切换','warn')}${btn('rollback','回退到双写（保留写入）')}</div>`);
  if(p==='integer') groups.push(btn('rollback','回退到双写（保留期间全部写入）'));
  groups.push(btn('refresh','刷新状态'));
  return groups.join('');
}

export async function render(){
  const role=localStorage.getItem('research-role');
  const s=await loadMigrationStatus(true);
  return `<div class="eyebrow">06 / STORAGE MIGRATION</div>
  <h1>工艺计数存储迁移 <small>浮点 JSON → 整数专用存储</small></h1>
  <p class="lead">在线回填、全量校验、原子切换与回退；迁移期间录入与研究发起不中断。</p>
  ${migrationBanner(s)}
  <div class="mig-console">
    <h2>迁移动作 ${phaseBadge(s)}</h2>
    <div class="mig-actions">${actions(s,role)}</div>
    <div id="mig-result" class="mig-result" role="status"></div>
  </div>
  <h2>校验差异明细</h2>
  ${s.diffs&&s.diffs.length
    ? table(['类型','批次','源记录','旧浮点','整数存储'],s.diffs.map(d=>[d.type,d.study_id,d.source_key,JSON.stringify(d.legacy??''),JSON.stringify(d.integer??'')]))
    : '<p class="method-note">暂无差异。差异存在时切换会被拒绝，可回退或更正数据后重试。</p>'}
  <details><summary>阶段与不变量说明</summary>
  <pre>${esc([
    'legacy：迁前旧浮点表唯一可读写。',
    'dual：在线回填 + 双写；读取仍依据旧表，新增/更正/移除同时落两存储，移除立墓碑防复活。',
    'switching：原子切换窗口；拒绝新研究冻结，数据写入继续。',
    'integer：整数表为读取依据；旧表继续同步，支持一键回退，回退保留期间写入。',
    '已完成研究的冻结快照与 SHA256 不随迁移改变；中断后重跑回填按游标续跑且不重复。',
  ].join('\n'))}</pre></details>`;
}

export function bind(refresh){
  const run=async(action)=>{
    const box=document.getElementById('mig-result');
    box.textContent='执行中…';
    try{
      let out;
      if(action==='backfill') out=await migration.backfill(500);
      else if(action==='verify') out=await migration.verify();
      else if(action==='beginSwitch') out=await migration.beginSwitch();
      else if(action==='completeSwitch') out=await migration.completeSwitch();
      else if(action==='rollback') out=await migration.rollback();
      else if(action==='start') out=await migration.start();
      clearMigrationCache();
      const s=await loadMigrationStatus(true);
      box.textContent=(out?JSON.stringify(out):'已刷新')+' · '+s.detail;
      refresh();
    }catch(e){box.textContent='操作失败：'+e.message;}
  };
  document.querySelectorAll('[data-mig]').forEach(el=>{el.onclick=()=>run(el.dataset.mig);});
}
