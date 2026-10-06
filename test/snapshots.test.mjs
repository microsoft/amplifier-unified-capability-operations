import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';import {join} from 'node:path';
const {createOperationsCapabilities}=await import(process.env.SNAPSHOT_NODE_MODULE??'../dist/index.js');
const python=process.env.OPERATIONS_OWNER_PYTHON;
test('installed public recovery lease seals owner stores and retires capture after release',{skip:!python},async()=>{
 const root=await mkdtemp(join(tmpdir(),'snapshot-operations-')),config=join(root,'config.json');await writeFile(config,JSON.stringify({dataDir:join(root,'owner')}));
 const owner=createOperationsCapabilities({owner:{command:python,args:['-I','-m','amplifier_unified_operations.server','--config',config]}});
 const context={fenceId:'snapshot',commandId:'recovery',purpose:'recovery',instanceId:'source',dataScope:'private'};
 try{
  const participant=owner.quiescenceParticipant('operations');const lease=await participant.acquire(context);
  assert.equal(typeof lease.captureSnapshot,'function');
  const receipt=await lease.captureSnapshot({commandId:'capture-one',privateContentReviewed:true});
  assert.equal(receipt.status,'captured');assert.equal(receipt.completeProductBackup,false);
  assert.deepEqual((await owner.inspectOwnerSnapshot('capture-one')).receipt,receipt);
  const raw=await owner.readOwnerSnapshot({commandId:'capture-one',store:'intake',offset:0,limit:16});
  assert.equal(Buffer.from(raw.data,'base64').toString(),'SQLite format 3\0');
  await lease.release('unknown');
  await assert.rejects(lease.captureSnapshot({commandId:'capture-two',privateContentReviewed:true}),/no longer live/);
  await participant.reconcileRelease({...context,outcome:'unchanged',proof:{verified:true,...context,outcome:'unchanged',receiptId:'original-snapshot-receipt'}});
  assert.equal((await owner.inspectOwnerSnapshot('capture-one')).receipt.status,'captured');
 }finally{await owner.close();await rm(root,{recursive:true,force:true})}
});
