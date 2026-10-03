import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {randomUUID} from 'node:crypto';

const available=['OPERATIONS_NATIVE_HOST_MODULE','OPERATIONS_NATIVE_NODE_MODULE','OPERATIONS_OWNER_PYTHON','ACP_EXECUTABLE','ACP_NATIVE_TEST_CONFIG','ACP_NATIVE_TEST_WORKSPACE'].every(key=>process.env[key]);
test('installed operations + AHP host + actual Core/Foundation: process, questions, follow-up and restart', {skip:!available,timeout:120000},async()=>{
 const {createHost}=await import(process.env.OPERATIONS_NATIVE_HOST_MODULE);
 const {createOperationsCapabilities}=await import(process.env.OPERATIONS_NATIVE_NODE_MODULE);
 const directory=await mkdtemp(join(tmpdir(),'operations-native-'));
 const workspace=process.env.ACP_NATIVE_TEST_WORKSPACE,config=join(directory,'owner.json');
 await writeFile(config,JSON.stringify({dataDir:join(directory,'operations')}));
 let host,owner,lostAnswer=false,answers=0,observations=0;const nativeEvents=[];
 const options={stateDirectory:join(directory,'host'),allowedWorkspaceRoots:[workspace],engines:[{id:'amplifier',label:'Amplifier',command:process.env.ACP_EXECUTABLE,args:['--config',process.env.ACP_NATIVE_TEST_CONFIG]}],
  nativeHostCapabilities:{version:1,features:{operations:true,questions:true}},nativeHostRequest:async(context,request)=>{
   if(request.operation==='operations.observe'){observations++;return owner.observe(context.session,context.nativeSessionId,request.args.event);}
   if(request.operation==='questions.delivery.admit')return owner.authorizeQuestionDelivery(context.session,request.args);
   if(request.operation==='questions.dependency')return owner.questionDependency(context.session,request.args.id);
   throw Error('Unexpected public native request '+request.operation);
  },nativeEvent:async(_context,event)=>nativeEvents.push(event),agentStopped:async event=>owner.interrupted(event.session)};
 const makeOwner=()=>createOperationsCapabilities({owner:{command:process.env.OPERATIONS_OWNER_PYTHON,args:['-I','-m','amplifier_unified_operations.server','--config',config]},
  inspectSession:(...args)=>host.inspectSession(...args),readSessionContext:(...args)=>host.readSessionContext(...args),readUserMessage:(...args)=>host.readUserMessage(...args),
  nativeControl:(...args)=>host.nativeControl(...args),nativeControlExisting:(...args)=>host.nativeControlExisting(...args),createSession:(...args)=>host.createSession(...args),
  submitScheduled:(...args)=>host.submitScheduled(...args),waitForTurn:(...args)=>host.waitForTurn(...args),
  submitQuestionAnswer:async(...args)=>{answers++;const result=await host.submitQuestionAnswer(...args);if(lostAnswer){await host.waitForTurn(args[0],args[1].commandId,30000);throw Error('Injected lost reply after confirmed native execution');}return result;}});
 const sid='ahp-session:/'+randomUUID();
 const action=async(operation,args={},commandId=randomUUID())=>(await owner.action({version:1,channel:sid,topic:operation.startsWith('schedule.')?'schedules':operation.startsWith('question.')?'questions':'operations',operation,args,commandId},{session:sid,clientId:'installed-owner',origin:'ui'})).result;
 const until=async check=>{const deadline=Date.now()+20000;while(Date.now()<deadline){const result=await check();if(result)return result;await new Promise(resolve=>setTimeout(resolve,30));}throw Error('Bounded wait timed out');};
 try{
  host=await createHost(options);owner=makeOwner();
  // Public managed creation is not needed: create the session through official AHP.
  const {AhpClient}=await import('@microsoft/agent-host-protocol/client');
  const {WebSocketTransport}=await import('@microsoft/agent-host-protocol/ws');
  const client=new AhpClient(await WebSocketTransport.connect(host.url));client.connect();
  await client.initialize({clientId:'installed-owner',protocolVersions:['0.9.0']});
  await client.request('createSession',{channel:sid,provider:'amplifier',workingDirectories:[pathToFileURL(workspace).href]});await client.shutdown();
  assert.deepEqual((await action('question.list')).items,[]);
  const created=await action('question.create',{prompt:'Which color?',required:true,dependency:'Paint choice'});
  const args={id:created.id,expectedRevision:1,text:'Blue'},command='saved-answer';
  const answer=await action('question.answer',args,command);assert.equal(answer.delivery.status,'accepted');
  const outcome=await host.waitForTurn(sid,answer.delivery.inputId,30000);assert.equal(outcome.status,'completed');assert.match(outcome.text,/Blue/);
  assert.equal((await host.readUserMessage(sid,answer.delivery.inputId)).inputOrigin,'question');
  assert.equal((await action('question.answer',args,command)).delivery.status,'accepted');assert.equal(answers,1);

  const follow='follow-'+randomUUID();await host.submitTurn(sid,{commandId:follow,text:'Explicit follow-up',clientId:'installed-owner',origin:'agent'});
  assert.equal((await host.waitForTurn(sid,follow,30000)).status,'completed');
  assert.equal((await host.readSessionContext(sid)).messages.find(row=>row.id===follow).inputOrigin,'coordination');
  await assert.rejects(host.readUserMessage(sid,follow),/not-user/);
  const slow='slow-'+randomUUID();await host.submitTurn(sid,{commandId:slow,text:'slow qualification',clientId:'installed-owner'});
  await until(async()=>nativeEvents.some(row=>row.event?.type==='generation.started'&&row.event?.activeInputId===slow));
  const busy=await action('question.create',{prompt:'During active work?',required:true,dependency:'Active turn choice'});
  const busyAnswer=await action('question.answer',{id:busy.id,expectedRevision:1,text:'Saved during active work'});
  assert.equal(busyAnswer.status,'answered');assert.equal(busyAnswer.delivery.status,'rejected');assert.match(busyAnswer.delivery.message,/running|busy/);
  await assert.rejects(host.readUserMessage(sid,busyAnswer.delivery.inputId),/unavailable/i);
  const interruption='interrupt-'+randomUUID();await host.interruptSession(sid,{commandId:interruption,turnId:slow,clientId:'installed-owner'});
  const interrupted=await host.waitForTurn(sid,slow,30000);assert.equal(interrupted.status,'cancelled',JSON.stringify(interrupted));
  const revision=(await host.inspectSession(sid)).interruptionRevision;
  await host.interruptSession(sid,{commandId:interruption,turnId:slow,clientId:'installed-owner'});assert.equal((await host.inspectSession(sid)).interruptionRevision,revision);

  const submitted=await action('operations.submit',{requestId:'process-admission',command:"python3 -u -c 'import sys; print(\"READY\",flush=True); print(sys.stdin.readline().strip(),flush=True)'"});
  assert.equal(submitted.state,'accepted',JSON.stringify(submitted));const processId=submitted.operationId;
  await until(async()=>{const row=await action('operations.read',{id:processId,maxBytes:4096});return JSON.stringify(row.chunks).includes('READY')&&row;});
  const written=await action('operations.write',{requestId:'process-input',id:processId,stdin:'typed input\n',closeStdin:true});assert.equal(written.state,'accepted');
  const completed=await until(async()=>{const row=await action('operations.read',{id:processId,maxBytes:4096});return !['running','queued','cancel_requested'].includes(row.state)&&row;});
  assert.match(JSON.stringify(completed.chunks),/typed input/);
  const cancellable=await action('operations.submit',{requestId:'process-cancel',command:"python3 -u -c 'import time; print(\"WAITING\",flush=True); time.sleep(60)'"});
  await until(async()=>{try{return (await action('operations.status',{id:cancellable.operationId})).controlAvailable;}catch{return false;}});
  await action('operations.cancel',{id:cancellable.operationId});await until(async()=>{const row=await action('operations.status',{id:cancellable.operationId});return !['running','queued','cancel_requested'].includes(row.state)&&row;});assert.ok(observations>0);

  await host.nativeControl(sid,'task.create',{objective:'Finite installed schedule check',maxTurns:3,commandId:'native-task',expectedRevision:0});
  const scheduleArgs={prompt:'Scheduled installed check',kind:'monitor',spec:{kind:'once',timezone:'UTC',startAt:new Date(Date.now()+3000).toISOString()},notificationPolicy:'failures_only',missedRunPolicy:'latest'};
  const preview=await action('schedule.preview',scheduleArgs);
  const scheduled=await action('schedule.create',{...scheduleArgs,expectedRevision:0,previewHash:preview.previewHash},'create-schedule');
  await owner.start();
  const run=await until(async()=>{const page=await action('schedule.read',{id:scheduled.schedule.id});return page.runs.find(row=>row.phase==='completed');});
  assert.equal((await host.readUserMessage(sid,run.inputId)).inputOrigin,'scheduled');
  assert.deepEqual((await action('schedule.request',{requestId:'create-schedule'})).receipt.result,scheduled);

  lostAnswer=true;const uncertain=await action('question.create',{prompt:'Recover exact answer?',required:true,dependency:'Recovery'});
  const uncertainArgs={id:uncertain.id,expectedRevision:1,text:'Retain this answer'};
  const unknown=await action('question.answer',uncertainArgs,'lost-answer');assert.equal(unknown.delivery.status,'unknown');
  assert.equal((await host.waitForTurn(sid,unknown.delivery.inputId,30000)).status,'completed');
  const savedContext=await host.readSessionContext(sid),answerCount=answers;
  await host.close();await owner.close();host=await createHost(options);owner=makeOwner();lostAnswer=false;
  assert.equal(host.diagnostics().activeAgents,0);
  assert.equal((await action('question.answer',uncertainArgs,'lost-answer')).delivery.status,'unknown');assert.equal(answers,answerCount);
  assert.equal((await action('operations.request',{requestId:'process-admission'})).operationId,processId);
  await assert.rejects(action('operations.write',{requestId:'after-restart',id:processId,stdin:'No replay'}),/unavailable/);
  assert.equal(host.diagnostics().activeAgents,0);assert.deepEqual(await host.readSessionContext(sid),savedContext);
 }finally{await host?.close();await owner?.close();await rm(directory,{recursive:true,force:true});}
});
