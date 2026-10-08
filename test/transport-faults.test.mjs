import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, writeFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {OwnerConnection} from '../dist/index.js';

async function peer(t, body, options={}) {
  const directory=await mkdtemp(join(tmpdir(),'operations-transport-'));
  const path=join(directory,'peer.mjs');
  await writeFile(path, `import {createInterface} from 'node:readline';
    const reply=(id,result)=>process.stdout.write(JSON.stringify({jsonrpc:'2.0',id,result})+'\\n');
    createInterface({input:process.stdin}).on('line',line=>{const row=JSON.parse(line);${body}});`);
  const owner=new OwnerConnection({command:process.execPath,args:[path],requestTimeoutMs:150,initializeTimeoutMs:1000,...options},async()=>({}),()=>{});
  t.after(async()=>{await owner.close();await rm(directory,{recursive:true,force:true});});
  return owner;
}
const initialize="if(row.method==='initialize'){reply(row.id,{protocolVersion:1});return;}";

test('silent initialization has a deadline and cannot start a replay',async t=>{
  const owner=await peer(t,'',{initializeTimeoutMs:75});
  await assert.rejects(owner.request('action',{}),/timed out.*unknown/);
  assert.equal(owner.admissionPending,0);
  await assert.rejects(owner.request('action',{}),/closed.*no automatic replay/);
});

test('a silent owner releases all bounded pending slots with unknown outcomes',async t=>{
  const owner=await peer(t,initialize);
  await owner.request('initialize',{});
  const pending=Array.from({length:64},()=>owner.request('action',{}));
  const settled=Promise.allSettled(pending);
  await assert.rejects(owner.request('action',{}),/capacity/);
  const outcomes=await settled;
  assert.equal(outcomes.length,64);
  for(const result of outcomes){assert.equal(result.status,'rejected');assert.match(result.reason.message,/timed out.*unknown/);}
  assert.equal(owner.admissionPending,0);
  await assert.rejects(owner.request('action',{}),/closed.*no automatic replay/);
});

for(const invalid of [null,[],{jsonrpc:'2.0',method:'owner/changed'},{jsonrpc:'2.0',method:'owner/changed',params:null},{jsonrpc:'2.0',id:2,error:{code:-1}},{jsonrpc:'2.0',id:2,result:{},error:{code:-1,message:'both'}}]){
  test(`malformed owner frame is contained: ${JSON.stringify(invalid)}`,async t=>{
    const owner=await peer(t,initialize+`process.stdout.write(${JSON.stringify(JSON.stringify(invalid)+'\n')});`);
    await assert.rejects(owner.request('action',{}),/Malformed owner response.*unknown/);
    assert.equal(owner.admissionPending,0);
    await assert.rejects(owner.request('action',{}),/closed/);
  });
}

test('closing a previously exited owner does not wait for the kill fallback',async t=>{
  const owner=await peer(t,initialize+'process.exit(0);');
  await assert.rejects(owner.request('action',{}),/exited/);
  const before=performance.now();await owner.close();
  assert.ok(performance.now()-before<500,'already exited child closes immediately');
});

test('completed calls clear deadlines and keep the healthy channel usable',async t=>{
  const owner=await peer(t,initialize+'reply(row.id,{ok:true});');
  assert.deepEqual(await owner.request('action',{}),{ok:true});
  await new Promise(resolve=>setTimeout(resolve,200));
  assert.equal(owner.admissionPending,0);
  assert.deepEqual(await owner.request('action',{}),{ok:true});
});

test('only finite bounded owner deadlines are accepted',()=>{
  for(const requestTimeoutMs of [0,24,90001,NaN,Infinity])assert.throws(()=>new OwnerConnection({command:process.execPath,requestTimeoutMs},async()=>{},()=>{}),/timeout/);
});
