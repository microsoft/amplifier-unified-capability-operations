import {forwardAdmissionAbort} from './admission-abort.js';
import {managedParticipant} from './managed-files.js';
import {retentionRequest,retentionParticipant} from './retention.js';
import {serviceIdentity,validateServiceRelease,type ServiceIdentity,type ServiceReleaseFields} from './service-lifecycle.js';
export type {ServiceIdentity,ServiceReleaseFields} from './service-lifecycle.js';
import {spawn,type ChildProcessWithoutNullStreams} from 'node:child_process';
import {createInterface} from 'node:readline';
export type Json=Record<string,any>;
export interface Context {clientId:string;origin?:'ui'|'agent';session?:string|{uri:string};}
export interface Launcher {command:string;args?:string[];env?:Record<string,string>;cwd?:string;requestTimeoutMs?:number;initializeTimeoutMs?:number;}
export interface Options {
 owner:Launcher;
 inspectSession:(uri:string,context?:{clientId:string})=>Promise<Json>;
 readSessionContext:(uri:string,limit:number)=>Promise<{messages:Json[];truncated?:boolean}>;
 readUserMessage?:(uri:string,id:string)=>Promise<Json|null>;
 nativeControl:(uri:string,operation:string,args:Json)=>Promise<Json>;
 nativeControlExisting:(uri:string,operation:string,args:Json)=>Promise<Json>;
 createSession:(input:Json)=>Promise<Json>;
 submitScheduled:(uri:string,input:Json)=>Promise<Json>;
 waitForTurn:(uri:string,commandId:string,waitMs?:number)=>Promise<Json>;
 submitQuestionAnswer?:(uri:string,input:Json)=>Promise<Json>;
 submitObservation?:(uri:string,input:Json)=>Promise<Json>;
 readTaskState?:(uri:string)=>Promise<Json>;
 qualifiedObservation?:(uri:string,operation:'qualify'|'check'|'read',args:Json)=>Promise<Json>;
 questionDependency?:(uri:string,id:string)=>Promise<Json>;
 operationSources?:(uri:string,options:{id?:string;limit:number;cursor?:[number,string]})=>Promise<Json[]>;
 notifySchedule?:(uri:string,run:Json)=>Promise<void>;
 onInvalidate?:(topic:string,scope:string)=>void;
 onMayBeIdle?:()=>void;
}
const METHODS=new Set(['inspectSession','nativeControl','nativeControlExisting','readUserMessage','createSession','submitScheduled','waitForTurn','submitQuestionAnswer','submitObservation','readTaskState','qualifiedObservation','questionDependency','operationSources','notifySchedule']);
const names={schedules:['list','read','request','preview','create','update','pause','resume','cancel','report','reconcile'].map(n=>'schedule.'+n),operations:['list','status','read','wait','submit','write','request','cancel','history.list','history.read'].map(n=>'operations.'+n),questions:['create','list','read','answer','cancel','supersede'].map(n=>'question.'+n),observations:['qualify','observers','preview','create','request','list','read','report','pause','cancel','resume'].map(n=>'observation.'+n)};
const sessionOf=(context:Context)=>typeof context.session==='string'?context.session:context.session?.uri;
/** Two-way owner channel. Calls are never retried after lost transport. */
export class OwnerConnection {
 private process?:ChildProcessWithoutNullStreams;private ready?:Promise<void>;private next=0;private closed=false;private pending=new Map<number,{resolve:(r:any)=>void;reject:(e:Error)=>void}>();
 get admissionPending(){return this.pending.size;}
 constructor(private launcher:Launcher,private callback:(method:string,args:Json)=>Promise<any>,private changed:(scope:string,topic?:string)=>void,private idle:()=>void=()=>{}){
  for(const timeout of [launcher.requestTimeoutMs,launcher.initializeTimeoutMs])if(timeout!==undefined&&(!Number.isInteger(timeout)||timeout<25||timeout>90000))throw Error('Owner timeout must be25–90000ms');
 }
 private fail(message:string){this.closed=true;for(const entry of this.pending.values())entry.reject(Error(message));this.pending.clear();}
 private write(row:Json){const data=JSON.stringify(row)+'\n';if(Buffer.byteLength(data)>4_000_000)throw Error('Owner frame exceeds4MB');if(this.closed||!this.process)throw Error('Owner unavailable; uncertain work was not replayed');this.process.stdin.write(data,error=>{if(error)this.fail('Owner transport failed; outcome unknown.');});}
 private start(){
  if(this.closed)throw Error('Owner connection is closed; no automatic replay.');
  if(!this.ready)this.ready=(async()=>{
   const child=this.process=spawn(this.launcher.command,this.launcher.args??[],{cwd:this.launcher.cwd,env:{...process.env,...this.launcher.env},stdio:'pipe'});
   child.stdin.on('error',()=>{this.fail('Owner input transport failed; outcome unknown');child.kill();});child.stderr.on('data',()=>{});child.on('error',()=>this.fail('Operation owner failed to start'));child.on('exit',()=>this.fail('Operation owner exited; uncertain commands not replayed'));
   let bytes=0;child.stdout.on('data',(chunk:Buffer)=>{for(const b of chunk){bytes=b===10?0:bytes+1;if(bytes>4_000_000){this.fail('Owner frame exceeded limit');child.kill();return;}}});
   createInterface({input:child.stdout}).on('line',line=>{void this.receive(line).catch(()=>{this.fail('Malformed owner response; outcome unknown, not replayed');child.kill();});});
   const value=await this.send('initialize',{},this.launcher.initializeTimeoutMs??5000);if(value?.protocolVersion!==1){this.fail('Unsupported operations owner');child.kill();throw Error('Unsupported operations owner');}
  })();return this.ready;
 }
 private async receive(line:string){
  let row:Json;try{row=JSON.parse(line);}catch{this.fail('Invalid owner response');this.process?.kill();return;}
  if(!row||typeof row!=='object'||Array.isArray(row)||row.jsonrpc!=='2.0')throw Error('Malformed owner envelope');
  if(row.method==='owner/idle'){this.idle();return;}
  if(row.method==='owner/changed'){
   if(!row.params||typeof row.params!=='object'||Array.isArray(row.params)||typeof row.params.session!=='string'||(row.params.topic!==undefined&&typeof row.params.topic!=='string'))throw Error('Malformed owner notification');
   this.changed(row.params.session,row.params.topic);return;
  }
  if(row.method){
   if(typeof row.method!=='string'||!['string','number'].includes(typeof row.id)||(row.params!==undefined&&(!row.params||typeof row.params!=='object'||Array.isArray(row.params))))throw Error('Malformed owner callback');
   const method=String(row.method).replace(/^host\//,'');
   try{if(!String(row.method).startsWith('host/')||!METHODS.has(method))throw Error('Unknown host callback');const result=await this.callback(method,row.params??{});this.write({jsonrpc:'2.0',id:row.id,result:result??null});}
   catch(error){if(!this.closed)this.write({jsonrpc:'2.0',id:row.id,error:{code:-32000,message:String(error instanceof Error?error.message:error)}});}return;
  }
  if(!Number.isSafeInteger(row.id)||Object.hasOwn(row,'result')===Object.hasOwn(row,'error')||(Object.hasOwn(row,'error')&&(!row.error||typeof row.error!=='object'||typeof row.error.message!=='string'||!Number.isInteger(row.error.code))))throw Error('Malformed owner reply');
  const entry=this.pending.get(row.id);if(!entry)return;this.pending.delete(row.id);row.error?entry.reject(Object.assign(Error(row.error.message),{refusal:row.error.data?.kind==='schedule_refused'&&row.error.data?.executed===false})):entry.resolve(row.result);
 }
 private send(method:string,params:Json,timeoutMs=this.launcher.requestTimeoutMs??90000){
  if(this.closed)throw Error('Owner unavailable; uncertain work was not replayed');
  if(this.pending.size>=64)throw Error('Owner request capacity reached');
  const id=++this.next;
  return new Promise<any>((resolve,reject)=>{
   // A timed-out mutation may already have executed. Terminalize this channel;
   // retain durable receipts/fences for explicit inspection after owner restart.
   const timer=setTimeout(()=>{this.fail('Owner reply timed out; outcome unknown, not replayed');this.process?.kill();},timeoutMs);timer.unref();
   const settle={resolve:(value:any)=>{clearTimeout(timer);resolve(value);},reject:(error:Error)=>{clearTimeout(timer);reject(error);}};
   this.pending.set(id,settle);
   try{this.write({jsonrpc:'2.0',id,method,params});}catch(e){this.pending.delete(id);settle.reject(e as Error);}
  });
 }
 async request(method:string,args:Json){await this.start();return this.send(method,args);}
 async close(){
  const child=this.process;this.fail('Owner closed; uncertain work was not replayed');
  if(!child||child.exitCode!==null||child.signalCode!==null)return;
  child.stdin.end();await new Promise<void>(resolve=>{const timer=setTimeout(()=>{child.kill('SIGKILL');resolve();},4000);child.once('exit',()=>{clearTimeout(timer);resolve();});});
 }
}
export interface QuiescenceContext {fenceId:string;commandId:string;purpose:'distribution-update'|'recovery'|'service-stop'|'retention-hide'|'managed-files-disposal';instanceId:string;dataScope:string;serviceIdentity?:ServiceIdentity;}
export interface QuiescenceProof extends Partial<ServiceReleaseFields> {verified:true;fenceId:string;commandId:string;outcome:'ready'|'unchanged';instanceId:string;dataScope:string;receiptId:string;}
const quiescenceReads=new Set(['schedule.list','schedule.read','schedule.request','operations.list','operations.status','operations.read','operations.wait','operations.request','operations.history.list','operations.history.read','question.list','question.read','observation.observers','observation.request','observation.list','observation.read','observation.report']);
export class OperationsCapabilities {
 readonly manifest:{version:number;topics:Record<string,Json>;actions:Record<string,Json>};
 readonly quiescenceAccess:Record<string,'read'>;
 private owner:OwnerConnection;private revision=0;
 constructor(private options:Options){const topics=Object.keys(names).filter(topic=>topic!=='observations'||options.qualifiedObservation&&options.readTaskState&&options.submitObservation);this.manifest={version:1,topics:Object.fromEntries(topics.map(topic=>[topic,{uri:`amplifier-capability://operations/${topic}`,version:1,watch:true}])),actions:Object.fromEntries(topics.flatMap(topic=>names[topic as keyof typeof names].map(operation=>[operation,{topic,operation,method:'x-amplifier/capabilityAction'}])))};this.quiescenceAccess=Object.fromEntries([...quiescenceReads].filter(name=>name in this.manifest.actions).map(name=>[name,'read']));this.owner=new OwnerConnection(options.owner,(m,p)=>this.callback(m,p),(scope,topic)=>{for(const selected of topic&&topic in this.manifest.topics?[topic]:topics)options.onInvalidate?.(selected,scope);},()=>options.onMayBeIdle?.());}
 private async callback(method:string,args:Json):Promise<any>{
  const sid=args.session as string;
  switch(method){
   case 'inspectSession':return this.options.inspectSession(sid);
   case 'nativeControl':return this.options.nativeControl(sid,args.operation,args.args);
   case 'nativeControlExisting':return this.options.nativeControlExisting(sid,args.operation,args.args);
   case 'createSession':return this.options.createSession(args);
   case 'submitScheduled':return this.options.submitScheduled(sid,args.input);
   case 'waitForTurn':return this.options.waitForTurn(sid,args.commandId,args.waitMs);
   case 'submitQuestionAnswer':return this.options.submitQuestionAnswer?.(sid,args.input)??{accepted:false,executed:false,reason:'Question delivery is unavailable; the answer remains saved'};
   case 'submitObservation':if(!this.options.submitObservation)throw Error('Observation admission is unavailable');return this.options.submitObservation(sid,args.input);
   case 'readTaskState':if(!this.options.readTaskState)throw Error('Passive task authority is unavailable');return this.options.readTaskState(sid);
   case 'qualifiedObservation':if(!this.options.qualifiedObservation)throw Error('Qualified observation reader is unavailable');return this.options.qualifiedObservation(sid,args.operation,args.args);
   case 'readUserMessage':{
    if(typeof args.id!=='string'||!args.id)return null;
    const value=this.options.readUserMessage?await this.options.readUserMessage(sid,args.id):(await this.options.readSessionContext(sid,200)).messages.find(r=>r.id===args.id)??null;
    if(!value)return null;const at=typeof value.createdAt==='string'?Date.parse(value.createdAt)/1000:value.createdAt;return {...value,createdAt:Number.isFinite(at)?at:0};
   }
   case 'questionDependency':if(!this.options.questionDependency)throw Error('Question owner unavailable; dependent work was not admitted');return this.options.questionDependency(sid,args.id);
   case 'operationSources':return this.options.operationSources?.(sid,{id:args.id,limit:args.limit,cursor:args.cursor})??[];
   case 'notifySchedule':await this.options.notifySchedule?.(sid,args.run);return {};
   default:throw Error('Unknown callback');
  }
 }
 private async authorize(scope:string,context:Context){if(typeof scope!=='string'||!scope.startsWith('ahp-session:/')||scope.length>8192)throw Error('Selected session scope required');if(context.session&&sessionOf(context)!==scope)throw Error('Session scope mismatch');await this.options.inspectSession(scope,{clientId:context.clientId});}
 private record(topic:string,scope:string,data:Json){if(Buffer.byteLength(JSON.stringify(data))>768*1024)throw Error('Requested operation page exceeds768KiB; use narrower action page');return {topic,scope,revision:++this.revision,data};}
 /** Start only the indexed due scheduler, with no native runtimes on idle boot. */
 start=async()=>this.owner.request('start',{observations:!!this.manifest.topics.observations});
 actionSchemas=async()=>Object.fromEntries(Object.entries(await this.owner.request('actions',{})).filter(([name])=>name in this.manifest.actions));
 read=async(request:{uri:string;topic:string;scope:string;clientId:string})=>{if(!(request.topic in names))throw Error('Unknown operation topic');await this.authorize(request.scope,{clientId:request.clientId});return this.record(request.topic,request.scope,await this.owner.request('snapshot',{session:request.scope,topic:request.topic}));};
 action=async(request:Json,context:Context)=>{
  const scope=request.channel,advertised=this.manifest.actions[request.operation];if(request.version!==1||!advertised||advertised.topic!==request.topic)throw Error('Unadvertised operation action');await this.authorize(scope,context);
  // A peer schedule read keeps the caller channel and its selected conversation
  // intact. Only these two passive operations may target another registered chat.
  const peerRead=['schedule.list','schedule.read'].includes(request.operation)&&request.args?.sessionId!==undefined&&request.args.sessionId!==scope;
  const target=peerRead?request.args.sessionId:scope;
  if(peerRead)await this.authorize(target,{clientId:context.clientId});
  let result:Json;
  try{result=await this.owner.request('action',{session:target,operation:request.operation,args:request.args??{},origin:context.origin??'ui',clientId:context.clientId,commandId:request.commandId});}
  catch(error){if(request.operation.startsWith('schedule.')&&(error as Error&{refusal?:boolean}).refusal)return {accepted:false,result:{executed:false,reason:(error as Error).message},updates:[],invalidate:[]};throw error;}
  if(peerRead)result={...result,sessionId:target,readOnly:true};
  const read=/\.(list|read|wait|status|request|preview|report|observers)$/.test(request.operation);
  return {accepted:true,result,updates:read?[]:[this.record(request.topic,scope,await this.owner.request('snapshot',{session:scope,topic:request.topic}))],invalidate:[],_meta:{'amplifier.dev/operation':{id:request.commandId}}};
 };
 /** Only authenticated native host bridge may supply runtime identity/producer event. */
 observe=async(session:string,nativeSessionId:string,event:Json)=>{await this.options.inspectSession(session);return this.owner.request('observe',{session,nativeSessionId,event});};
 interrupted=async(session:string)=>this.owner.request('interrupted',{session});
 /** Bounded indexed pending IDs for selected coordination; no question body materialization. */
 readQuestionAttention=async(session:string)=>{await this.options.inspectSession(session);return this.owner.request('questionAttention',{session});};
 questionDependency=async(session:string,id:string)=>{await this.options.inspectSession(session);return this.owner.request('questionDependency',{session,id});};
 authorizeQuestionDelivery=async(session:string,args:Json)=>{await this.options.inspectSession(session);return this.owner.request('questionDelivery',{session,args});};
 authorizeObservation=async(session:string,args:Json)=>{await this.options.inspectSession(session);return this.owner.request('observationAdmission',{session,args});};
 /** Trusted portability composition verifies signed pairing and release before these calls. */
 exportTransferEvidence=async(args:Json)=>{await this.options.inspectSession(args.session);return this.owner.request('exportTransferEvidence',args);};
 stageTransferEvidence=async(args:Json)=>this.owner.request('stageTransferEvidence',args);
 activateTransferEvidence=async(args:Json)=>this.owner.request('activateTransferEvidence',args);
 readTransferEvidence=async(args:Json)=>{await this.options.inspectSession(args.session);return this.owner.request('readTransferEvidence',args);};
 /** Trusted archive coordinator only; inspection never retries capture or activates a restore. */
 inspectOwnerSnapshot=(commandId:string)=>this.owner.request('snapshot.inspect',{commandId});
 readOwnerSnapshot=(args:{commandId:string;store:string;offset:number;limit:number})=>this.owner.request('snapshot.read',args);
 /** Trusted composition only. The private Python owner closes intake durably before returning. */
 quiescenceParticipant(ownerId:string){
  if(!ownerId||ownerId.length>200)throw Error('Bounded owner identity required');
  const release=async(context:QuiescenceContext,outcome:'ready'|'unchanged'|'unknown',proof?:QuiescenceProof|{kind:'admission-refused'})=>{
   if(context.purpose==='service-stop'&&outcome!=='unknown'&&proof?.kind!=='admission-refused')validateServiceRelease(context,outcome,proof);
   const result=await this.owner.request('quiescence.release',{...context,outcome,proof});
   if(outcome!=='unknown'&&result.released!==true)throw Error('Owner intake release is not confirmed');
  };
  return managedParticipant(retentionParticipant({id:ownerId,serviceStop:{version:1 as const},acquire:async(context:Readonly<QuiescenceContext>)=>{
   const exact:QuiescenceContext=structuredClone(context);if(exact.purpose==='retention-hide'&&(await this.owner.request('initialize',{})).quiescence?.retentionHide?.version!==1)return null;if(exact.purpose==='service-stop'){exact.serviceIdentity=serviceIdentity(exact.serviceIdentity);if(exact.serviceIdentity.instanceId!==exact.instanceId||exact.serviceIdentity.dataScope!==exact.dataScope)throw Error('Service identity differs from operation context');const hello=await this.owner.request('initialize',{});if(hello.quiescence?.serviceStop?.version!==1)return null;}const result=await this.owner.request('quiescence.acquire',exact);
   if(result.acquired!==true)return null;
   if(result.fenceId!==exact.fenceId||result.intakeClosed!==true)throw Error('Owner intake acquisition is not confirmed');
   let live=true;const snapshots=(await this.owner.request('initialize',{})).quiescence?.snapshots?.version===1;
   return {ownerId,fenceId:exact.fenceId,...(exact.purpose==='recovery'&&snapshots?{captureSnapshot:async(args:{commandId:string;privateContentReviewed:true})=>{if(!live)throw Error('Snapshot recovery lease is no longer live');return this.owner.request('quiescence.snapshot',{...args,context:exact});}}:{}),inspectRetentionReferences:(args:any)=>this.owner.request('quiescence.retention',retentionRequest(exact,args)),release:(outcome:'ready'|'unchanged'|'unknown',proof?:QuiescenceProof|{kind:'admission-refused'})=>{live=false;return release(exact,outcome,proof);}};
  },abortAdmission:async(context:any)=>{if(this.owner.admissionPending)throw Error('Owner requests are still in flight');return forwardAdmissionAbort((method,params)=>this.owner.request(method,params),context,ownerId);},reconcileRelease:async(context:Readonly<QuiescenceContext>&{outcome:'ready'|'unchanged';proof:QuiescenceProof})=>{if(context.purpose==='service-stop')validateServiceRelease(context,context.outcome,context.proof);return release(context,context.outcome,context.proof);}},args=>this.owner.request('quiescence.retention',args)),args=>this.owner.request('quiescence.managedFiles',args),async()=>(await this.owner.request('initialize',{})).quiescence?.managedFiles?.version===1);
 }
 close=async()=>this.owner.close();
}
export function createOperationsCapabilities(options:Options){return new OperationsCapabilities(options);}
