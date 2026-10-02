import {spawn,type ChildProcessWithoutNullStreams} from 'node:child_process';
import {createInterface} from 'node:readline';
export type Json=Record<string,any>;
export interface Context {clientId:string;origin?:'ui'|'agent';session?:string|{uri:string};}
export interface Launcher {command:string;args?:string[];env?:Record<string,string>;cwd?:string;}
export interface Options {
 owner:Launcher;
 inspectSession:(uri:string,context?:{clientId:string})=>Promise<Json>;
 readSessionContext:(uri:string,limit:number)=>Promise<{messages:Json[];truncated?:boolean}>;
 readUserMessage?:(uri:string,id:string)=>Promise<Json|null>;
 nativeControl:(uri:string,operation:string,args:Json)=>Promise<Json>;
 nativeControlExisting:(uri:string,operation:string,args:Json)=>Promise<Json>;
 createSession:(input:Json)=>Promise<Json>;
 submitScheduled:(uri:string,input:Json)=>Promise<Json>;
 waitForTurn:(uri:string,commandId:string)=>Promise<Json>;
 submitQuestionAnswer?:(uri:string,input:Json)=>Promise<Json>;
 questionDependency?:(uri:string,id:string)=>Promise<Json>;
 operationSources?:(uri:string,options:{id?:string;limit:number;cursor?:[number,string]})=>Promise<Json[]>;
 notifySchedule?:(uri:string,run:Json)=>Promise<void>;
 onInvalidate?:(topic:string,scope:string)=>void;
}
const METHODS=new Set(['inspectSession','nativeControl','nativeControlExisting','readUserMessage','createSession','submitScheduled','waitForTurn','submitQuestionAnswer','questionDependency','operationSources','notifySchedule']);
const names={schedules:['list','read','preview','create','update','pause','resume','cancel','report','reconcile'].map(n=>'schedule.'+n),operations:['list','status','read','wait','submit','write','request','cancel'].map(n=>'operations.'+n),questions:['create','list','read','answer','cancel','supersede'].map(n=>'question.'+n)};
const sessionOf=(context:Context)=>typeof context.session==='string'?context.session:context.session?.uri;
/** Two-way owner channel. Calls are never retried after lost transport. */
export class OwnerConnection {
 private process?:ChildProcessWithoutNullStreams;private ready?:Promise<void>;private next=0;private closed=false;private pending=new Map<number,{resolve:(r:any)=>void;reject:(e:Error)=>void}>();
 constructor(private launcher:Launcher,private callback:(method:string,args:Json)=>Promise<any>,private changed:(scope:string,topic?:string)=>void){}
 private fail(message:string){this.closed=true;for(const entry of this.pending.values())entry.reject(Error(message));this.pending.clear();}
 private write(row:Json){const data=JSON.stringify(row)+'\n';if(Buffer.byteLength(data)>4_000_000)throw Error('Owner frame exceeds4MB');if(this.closed||!this.process)throw Error('Owner unavailable; uncertain work was not replayed');this.process.stdin.write(data,error=>{if(error)this.fail('Owner transport failed; outcome unknown.');});}
 private start(){
  if(this.closed)throw Error('Owner connection is closed; no automatic replay.');
  if(!this.ready)this.ready=(async()=>{
   const child=this.process=spawn(this.launcher.command,this.launcher.args??[],{cwd:this.launcher.cwd,env:{...process.env,...this.launcher.env},stdio:'pipe'});
   child.stderr.on('data',()=>{});child.on('error',()=>this.fail('Operation owner failed to start'));child.on('exit',()=>this.fail('Operation owner exited; uncertain commands not replayed'));
   let bytes=0;child.stdout.on('data',(chunk:Buffer)=>{for(const b of chunk){bytes=b===10?0:bytes+1;if(bytes>4_000_000){this.fail('Owner frame exceeded limit');child.kill();return;}}});
   createInterface({input:child.stdout}).on('line',line=>{void this.receive(line);});
   const value=await this.send('initialize',{});if(value.protocolVersion!==1)throw Error('Unsupported operations owner');
  })();return this.ready;
 }
 private async receive(line:string){
  let row:Json;try{row=JSON.parse(line);}catch{this.fail('Invalid owner response');this.process?.kill();return;}
  if(row.method==='owner/changed'){this.changed(row.params.session,row.params.topic);return;}
  if(row.method){
   const method=String(row.method).replace(/^host\//,'');
   try{if(!String(row.method).startsWith('host/')||!METHODS.has(method))throw Error('Unknown host callback');const result=await this.callback(method,row.params??{});this.write({jsonrpc:'2.0',id:row.id,result:result??null});}
   catch(error){if(!this.closed)this.write({jsonrpc:'2.0',id:row.id,error:{code:-32000,message:String(error instanceof Error?error.message:error)}});}return;
  }
  const entry=this.pending.get(row.id);if(!entry)return;this.pending.delete(row.id);row.error?entry.reject(Error(row.error.message)):entry.resolve(row.result);
 }
 private send(method:string,params:Json){if(this.pending.size>=64)throw Error('Owner request capacity reached');const id=++this.next;return new Promise<any>((resolve,reject)=>{this.pending.set(id,{resolve,reject});try{this.write({jsonrpc:'2.0',id,method,params});}catch(e){this.pending.delete(id);reject(e as Error);}});}
 async request(method:string,args:Json){await this.start();return this.send(method,args);}
 async close(){if(!this.process){this.closed=true;return;}this.process.stdin.end();await new Promise<void>(resolve=>{const timer=setTimeout(()=>{this.process?.kill();resolve();},4000);this.process!.once('exit',()=>{clearTimeout(timer);resolve();});});this.fail('Owner closed');}
}
export class OperationsCapabilities {
 readonly manifest={version:1,topics:Object.fromEntries(Object.keys(names).map(topic=>[topic,{uri:`amplifier-capability://operations/${topic}`,version:1,watch:true}])),actions:Object.fromEntries(Object.entries(names).flatMap(([topic,values])=>values.map(operation=>[operation,{topic,operation,method:'x-amplifier/capabilityAction'}])))};
 private owner:OwnerConnection;private revision=0;
 constructor(private options:Options){this.owner=new OwnerConnection(options.owner,(m,p)=>this.callback(m,p),(scope,topic)=>{for(const selected of topic&&topic in names?[topic]:Object.keys(names))options.onInvalidate?.(selected,scope);});}
 private async callback(method:string,args:Json):Promise<any>{
  const sid=args.session as string;
  switch(method){
   case 'inspectSession':return this.options.inspectSession(sid);
   case 'nativeControl':return this.options.nativeControl(sid,args.operation,args.args);
   case 'nativeControlExisting':return this.options.nativeControlExisting(sid,args.operation,args.args);
   case 'createSession':return this.options.createSession(args);
   case 'submitScheduled':return this.options.submitScheduled(sid,args.input);
   case 'waitForTurn':return this.options.waitForTurn(sid,args.commandId);
   case 'submitQuestionAnswer':return this.options.submitQuestionAnswer?.(sid,args.input)??{accepted:false,executed:false,reason:'Question delivery is unavailable; the answer remains saved'};
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
 private async authorize(scope:string,context:Context){if(!scope?.startsWith('ahp-session:/'))throw Error('Selected session scope required');if(context.session&&sessionOf(context)!==scope)throw Error('Session scope mismatch');await this.options.inspectSession(scope,{clientId:context.clientId});}
 private record(topic:string,scope:string,data:Json){if(Buffer.byteLength(JSON.stringify(data))>768*1024)throw Error('Requested operation page exceeds768KiB; use narrower action page');return {topic,scope,revision:++this.revision,data};}
 /** Start only the indexed due scheduler, with no native runtimes on idle boot. */
 start=async()=>this.owner.request('start',{});
 actionSchemas=async()=>this.owner.request('actions',{});
 read=async(request:{uri:string;topic:string;scope:string;clientId:string})=>{if(!(request.topic in names))throw Error('Unknown operation topic');await this.authorize(request.scope,{clientId:request.clientId});return this.record(request.topic,request.scope,await this.owner.request('snapshot',{session:request.scope,topic:request.topic}));};
 action=async(request:Json,context:Context)=>{
  const scope=request.channel,advertised=this.manifest.actions[request.operation];if(request.version!==1||!advertised||advertised.topic!==request.topic)throw Error('Unadvertised operation action');await this.authorize(scope,context);
  const result=await this.owner.request('action',{session:scope,operation:request.operation,args:request.args??{},origin:context.origin??'ui',clientId:context.clientId,commandId:request.commandId});
  const read=/\.(list|read|wait|status|request|preview)$/.test(request.operation);
  return {accepted:true,result,updates:read?[]:[this.record(request.topic,scope,await this.owner.request('snapshot',{session:scope,topic:request.topic}))],invalidate:[],_meta:{'amplifier.dev/operation':{id:request.commandId}}};
 };
 /** Only authenticated native host bridge may supply runtime identity/producer event. */
 observe=async(session:string,nativeSessionId:string,event:Json)=>{await this.options.inspectSession(session);return this.owner.request('observe',{session,nativeSessionId,event});};
 interrupted=async(session:string)=>this.owner.request('interrupted',{session});
 questionDependency=async(session:string,id:string)=>{await this.options.inspectSession(session);return this.owner.request('questionDependency',{session,id});};
 authorizeQuestionDelivery=async(session:string,args:Json)=>{await this.options.inspectSession(session);return this.owner.request('questionDelivery',{session,args});};
 close=async()=>this.owner.close();
}
export function createOperationsCapabilities(options:Options){return new OperationsCapabilities(options);}
