import {request,post} from './client.js';
export const studies={list:()=>request('/studies'),create:x=>post('/studies',x),rows:id=>request(`/studies/${id}/observations`),save:(id,x,row)=>row?request(`/studies/${id}/observations/${row}`,{method:'PUT',body:JSON.stringify(x)}):post(`/studies/${id}/observations`,x),remove:(id,row)=>request(`/studies/${id}/observations/${row}`,{method:'DELETE'})};
export const runs={list:()=>request('/runs'),create:x=>post('/runs',x),get:id=>request(`/runs/${id}`),review:(id,note)=>post(`/runs/${id}/review`,{note}),export:id=>request(`/runs/${id}/export`)};
export const migration={
 status:()=>request('/migration/status'),
 start:()=>post('/migration/start'),
 backfill:(limit=500)=>post(`/migration/backfill?limit=${limit}`),
 verify:()=>post('/migration/verify'),
 beginSwitch:()=>post('/migration/switch/begin'),
 completeSwitch:()=>post('/migration/switch/complete'),
 rollback:()=>post('/migration/rollback'),
};
