import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {createOperationsCapabilities} from '../dist/index.js';
const sid='ahp-session:/fixture';
test('real Python owner protocol: scoped schedule review, process receipts and bounded reads',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'unified-operations-'));const config=join(directory,'config.json');await writeFile(config,JSON.stringify({dataDir:join(directory,'owner')}));
 const calls=[],invalidations=[];const inspect=async uri=>{assert.equal(uri,sid);return {uri,status:'idle',configurationHash:'fixture',interruptionRevision:0,executionRevision:0};};
 const provider=createOperationsCapabilities({owner:{command:resolve('python/.venv/bin/python'),args:['-m','amplifier_unified_operations.server','--config',config]},inspectSession:inspect,
  readSessionContext:async()=>({messages:[]}),nativeControl:async(uri,op,args)=>{calls.push({uri,op,args});if(op==='task.get')return {task:{id:'task',revision:1,status:'active'}};if(op==='operations.submit')return {success:true,output:{process_id:'process1',owner_id:'mount1',state:'running'}};throw Error(op);},
  nativeControlExisting:async()=>{throw Error('No live process owner');},createSession:async()=>{throw Error('Unused');},submitScheduled:async()=>{throw Error('Not due');},waitForTurn:async()=>{throw Error('Not admitted');},onInvalidate:(topic,scope)=>invalidations.push({topic,scope})});
 const invoke=(operation,args={},commandId=operation)=>provider.action({version:1,channel:sid,topic:operation.startsWith('schedule.')?'schedules':'operations',operation,args,commandId},{clientId:'fixture',session:sid,origin:'ui'});
 try{
  const schemas=await provider.actionSchemas();assert.ok(schemas['schedule.create'].schema.properties.previewHash);
  const read=await provider.read({uri:'amplifier-capability://operations/schedules',topic:'schedules',scope:sid,clientId:'fixture'});assert.deepEqual(read.data.schedules[sid],[]);assert.equal(calls.length,0);
  const args={prompt:'Explicit fixture monitoring',kind:'monitor',spec:{kind:'interval',timezone:'UTC',startAt:new Date(Date.now()+3600000).toISOString(),intervalSeconds:3600},notificationPolicy:'changes',missedRunPolicy:'latest'};
  const preview=(await invoke('schedule.preview',args)).result;
  const created=await invoke('schedule.create',{...args,expectedRevision:0,previewHash:preview.previewHash});assert.equal(created.result.schedule.status,'active');assert.equal(created.updates[0].data.schedules[sid].length,1);
  const receipt=await invoke('operations.submit',{requestId:'request1',command:'fixture only'});assert.equal(receipt.result.state,'accepted');
  const again=await invoke('operations.submit',{requestId:'request1',command:'fixture only'},'duplicate');assert.equal(again.result.operationId,'process1');assert.equal(calls.filter(c=>c.op==='operations.submit').length,1);
  await assert.rejects(invoke('operations.submit',{sessionId:'ahp-session:/elsewhere',requestId:'bad',command:'denied'}),/trusted context/);
  const observed=await provider.observe(sid,'native1',{schemaVersion:1,eventId:'p1:1',sequence:1,operationId:'p1',ownerId:'mount1',source:'tool-bash',kind:'process',phase:'started',at:1});assert.equal(observed.id,'p1');
  const status=await invoke('operations.status',{id:'p1'});assert.equal(status.result.runtimeSessionId,'native1');
  await provider.interrupted(sid);assert.equal((await invoke('operations.status',{id:'p1'})).result.state,'outcome_unknown');assert.ok(invalidations.length);
 }finally{await provider.close();await rm(directory,{recursive:true,force:true});}
});
