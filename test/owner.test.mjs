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

test('saved question answer proves authority through reentrant stdio and preserves a lost delivery without replay',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'unified-questions-')),config=join(directory,'config.json');await writeFile(config,JSON.stringify({dataDir:join(directory,'owner')}));
 let provider,deliveryCount=0,enteredResolve;const entered=new Promise(resolve=>{enteredResolve=resolve;});
 const make=(hanging=false)=>createOperationsCapabilities({owner:{command:resolve('python/.venv/bin/python'),args:['-m','amplifier_unified_operations.server','--config',config]},inspectSession:async()=>({uri:sid,status:'idle'}),
  readSessionContext:async()=>({messages:[]}),nativeControl:async()=>{throw Error('No generic native control');},nativeControlExisting:async()=>{throw Error('No native process');},createSession:async()=>{throw Error('Unused');},submitScheduled:async()=>{throw Error('Unused');},waitForTurn:async()=>{throw Error('Not completion');},
  submitQuestionAnswer:async(session,input)=>{deliveryCount++;const proof=await provider.authorizeQuestionDelivery(session,{...input,inputId:input.commandId});assert.equal(proof.admitted,true);if(hanging){enteredResolve();return new Promise(()=>{});}return {accepted:true};}});
 const invoke=(operation,args={},commandId=operation)=>provider.action({version:1,channel:sid,topic:'questions',operation,args,commandId},{clientId:'question-browser',session:sid,origin:'ui'});
 try{
  provider=make();const question=(await invoke('question.create',{prompt:'Which color?',required:true,dependency:'Paint selection'},'create-accepted')).result;
  assert.deepEqual((await provider.readQuestionAttention(sid)).questionIds,[question.id]);
  const args={id:question.id,expectedRevision:1,text:'Blue'};const answer=(await invoke('question.answer',args,'accepted-answer')).result;
  assert.deepEqual((await provider.readQuestionAttention(sid)).questionIds,[]);
  assert.equal(answer.delivery.status,'accepted');assert.equal((await provider.questionDependency(sid,question.id)).text,'Blue');
  assert.equal((await invoke('question.answer',args,'accepted-answer')).result.delivery.status,'accepted');assert.equal(deliveryCount,1);
  await provider.close();provider=make(true);
  const pending=(await invoke('question.create',{prompt:'Which finish?',required:true,dependency:'Finish selection'},'create-pending')).result;
  const pendingArgs={id:pending.id,expectedRevision:1,text:'Matte'};
  const inFlight=invoke('question.answer',pendingArgs,'uncertain-answer');const rejected=assert.rejects(inFlight,/closed|exited/i);await entered;await provider.close();await rejected;
  provider=make();const saved=(await invoke('question.read',{id:pending.id})).result;assert.equal(saved.status,'answered');assert.equal(saved.delivery.status,'unknown');
  assert.equal((await invoke('question.answer',pendingArgs,'uncertain-answer')).result.delivery.status,'unknown');assert.equal(deliveryCount,2,'Restart and duplicate must never resend');
 }finally{await provider?.close();await rm(directory,{recursive:true,force:true});}
});

test('qualified observation owner uses passive authority and exact typed handoff through real stdio',{timeout:15000},async()=>{
 const directory=await mkdtemp(join(tmpdir(),'unified-observation-')),config=join(directory,'config.json');await writeFile(config,JSON.stringify({dataDir:join(directory,'owner')}));
 let provider,submissions=0,passive=0;const state={uri:sid,status:'idle',nativeSessionId:'native',executionRevision:0,interruptionRevision:0,configurationHash:'config'};
 provider=createOperationsCapabilities({owner:{command:resolve('python/.venv/bin/python'),args:['-m','amplifier_unified_operations.server','--config',config]},inspectSession:async()=>({...state}),readTaskState:async()=>{passive++;return {available:true,revision:'r1',task:{id:'task',revision:1,status:'active',questionIds:[]}}},
  readSessionContext:async()=>({messages:[]}),nativeControl:async()=>{throw Error('Must not activate cold runtimes');},nativeControlExisting:async()=>({task:{id:'task'}}),createSession:async()=>{throw Error('No new sessions');},submitScheduled:async()=>{throw Error('No scheduled prompt fallback');},waitForTurn:async()=>({status:'completed',workContinues:false}),
  qualifiedObservation:async(_session,operation)=>operation==='qualify'?{id:'observer',requestArgument:'observation'}:operation==='check'?{eligible:true}:{status:'actionable',summary:'Exact fixture result',evidence:[]},
  submitObservation:async(session,input)=>{submissions++;state.executionRevision++;state.observationAdmission={...input,inputId:input.commandId,admittedExecutionRevision:state.executionRevision,status:'dispatching'};const args=Object.fromEntries(Object.entries(input).filter(([key])=>!key.startsWith('expected')&&key!=='commandId'));args.inputId=input.commandId;assert.equal((await provider.authorizeObservation(session,args)).admitted,true);return {accepted:true,turnId:input.commandId}}});
 const action=async(operation,args={})=>(await provider.action({version:1,channel:sid,topic:'observations',operation,args,commandId:crypto.randomUUID()},{clientId:'watch-browser',session:sid,origin:'ui'})).result;
 try{
  assert.ok((await provider.actionSchemas())['observation.preview']);await provider.start();
  await action('observation.qualify',{descriptor:{}});const args={observerId:'observer',target:{},scope:{},sourceId:'fixture://source',args:{},intervalSeconds:15,durationSeconds:60,readTimeoutSeconds:3};const preview=await action('observation.preview',args);
  const watch=(await action('observation.create',{...args,previewHash:preview.previewHash,requestId:'one-watch'})).watch;
  let report;const until=Date.now()+12000;
  do{report=await action('observation.read',{id:watch.id});if(report.handoffs[0]?.phase==='completed')break;await new Promise(resolve=>setTimeout(resolve,100));}while(Date.now()<until);
  assert.equal(report.handoffs[0]?.phase,'completed');assert.equal(submissions,1);assert.ok(passive>0);
  assert.equal((await action('observation.request',{requestId:'one-watch'})).watch.id,watch.id);
 }finally{await provider.close();await rm(directory,{recursive:true,force:true});}
});
