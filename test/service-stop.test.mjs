import test from 'node:test';import assert from 'node:assert/strict';import {mkdtemp,writeFile,readFile,rm} from 'node:fs/promises';import {tmpdir} from 'node:os';import {join,resolve} from 'node:path';
const identity={installationId:'installation',dataScope:'owned',ownerId:'retained-platform',instanceId:'host-one',releaseDigest:'signed-release'};
const c={fenceId:'service-fence',commandId:'stop-service',purpose:'service-stop',instanceId:'host-one',dataScope:'owned',serviceIdentity:identity};
const p={verified:true,fenceId:c.fenceId,commandId:c.commandId,outcome:'ready',instanceId:'host-two',dataScope:c.dataScope,receiptId:'platform-settlement',kind:'service-lifecycle',serviceOutcome:'resumed',expected:identity,observed:{...identity,instanceId:'host-two'},resumeCommandId:'resume',exitReceiptId:'owned-exit',readyReceiptId:'authenticated-ready'};
const {createOperationsCapabilities}=await import(process.env.OPERATIONS_NATIVE_NODE_MODULE??'../dist/index.js');
const python=process.env.OPERATIONS_OWNER_PYTHON??resolve('python/.venv/bin/python');
test('installed Python service participant survives SIGKILL, rejects generic authority and reuses exact confirmed release',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'service-rpc-')),config=join(directory,'owner.json'),pidfile=join(directory,'pid');await writeFile(config,JSON.stringify({dataDir:join(directory,'data')}));
 const code="import os,pathlib,runpy,sys;pathlib.Path(sys.argv.pop(1)).write_text(str(os.getpid()));runpy.run_module('amplifier_unified_operations.server',run_name='__main__')";
 const launcher={command:python,args:['-I','-c',code,pidfile,'--config',config]};const make=()=>createOperationsCapabilities({owner:launcher,inspectSession:async()=>({uri:'ahp-session:/one',status:'idle'}),readSessionContext:async()=>({messages:[]}),nativeControl:async()=>({success:true,output:{process_id:'fixture-process',owner_id:'fixture-runtime',state:'running'}}),nativeControlExisting:async()=>{throw Error('No existing runtime')},createSession:async()=>{throw Error('No create')},submitScheduled:async()=>{throw Error('No prompt')},waitForTurn:async()=>{throw Error('No turn')}});let owner=make();
 const action=(operation,args)=>owner.action({version:1,channel:'ahp-session:/one',topic:operation.startsWith('schedule.')?'schedules':'operations',operation,commandId:operation,args},{clientId:'fixture',session:'ahp-session:/one'});
 try{
  const participant=owner.quiescenceParticipant('operations');assert.equal(participant.serviceStop.version,1);const lease=await participant.acquire(c);assert.ok(lease);
  await action('schedule.list',{});await assert.rejects(action('operations.submit',{requestId:'effect',command:'fixture-no-process'}),/intake.*closed/);

  await assert.rejects(participant.reconcileRelease({...c,outcome:'unchanged',proof:{kind:'admission-refused'}}),/proof|identity/);
  const pid=Number(await readFile(pidfile,'utf8'));process.kill(pid,'SIGKILL');await new Promise(r=>setTimeout(r,40));await owner.close();owner=make();
  await assert.rejects(owner.quiescenceParticipant('operations').reconcileRelease({...c,outcome:'ready',proof:{...p,kind:'distribution-update'}}),/proof/);
  await assert.rejects(action('operations.submit',{requestId:'effect',command:'fixture-no-process'}),/intake.*closed/);await action('schedule.list',{});

  const exact={...c,outcome:'ready',proof:p};await owner.quiescenceParticipant('operations').reconcileRelease(exact);await owner.close();owner=make();await owner.quiescenceParticipant('operations').reconcileRelease(exact);
  await assert.rejects(owner.quiescenceParticipant('operations').reconcileRelease({...exact,proof:{...p,readyReceiptId:'changed'}}),/proof|receipt|fence/i);
  await action('operations.submit',{requestId:'effect',command:'fixture-no-process'});

 }finally{await owner.close();await rm(directory,{recursive:true,force:true})}
});
