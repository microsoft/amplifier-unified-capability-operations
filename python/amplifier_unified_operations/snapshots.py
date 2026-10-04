"""Trusted held Operations-store images; not a product archive or activation API."""
import base64,hashlib,json,os,time,stat
from pathlib import Path
from collections import OrderedDict

STORES=('intake','schedules','operations','questions','observations','transfer','legacy')
MAX_BYTES=64*1024*1024
MAX_READERS=8

def identity(info):
    return tuple(getattr(info,key) for key in ('st_dev','st_ino','st_mode','st_nlink','st_size','st_mtime_ns','st_ctime_ns'))

def support():
    try:
        from amplifier_operations.snapshots import VERSION
        return VERSION==1
    except ImportError:return False

def token(value):
    if not isinstance(value,str) or not 1<=len(value)<=200 or any(ord(c)<32 for c in value):raise ValueError('Bounded original snapshot command required')
    return value

def checked(path):
    path=Path(path)
    for part in (path,*path.parents):
        if part.is_symlink():raise ValueError('Snapshot owner paths cannot traverse symlinks')
    return path

def write(path,value):
    raw=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
    if len(raw)>32768:raise ValueError('Bounded snapshot receipt required')
    temporary=checked(path.with_suffix('.tmp'))
    with temporary.open('xb') as stream:os.chmod(temporary,0o600);stream.write(raw);stream.flush();os.fsync(stream.fileno())
    os.replace(temporary,path)
    if os.name!='nt':
        fd=os.open(path.parent,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)

class Snapshots:
    def __init__(self,owner):self.owner=owner;self.readers=OrderedDict()
    def close(self):
        for row in self.readers.values():row['stream'].close()
        self.readers.clear()
    def reader(self,key,path,sha):
        row=self.readers.pop(key,None)
        if row is not None:
            try:
                if identity(os.fstat(row['stream'].fileno()))!=row['identity'] or identity(path.stat())!=row['identity']:raise ValueError('Sealed snapshot image changed')
            except BaseException:row['stream'].close();raise
            self.readers[key]=row;return row
        while len(self.readers)>=MAX_READERS:self.readers.popitem(last=False)[1]['stream'].close()
        flags=os.O_RDONLY|getattr(os,'O_NONBLOCK',0)|getattr(os,'O_NOFOLLOW',0)
        stream=os.fdopen(os.open(path,flags),'rb')
        try:
            info=os.fstat(stream.fileno());stamp=identity(info)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size>MAX_BYTES:raise ValueError('Sealed snapshot image exceeds bounds')
            digest=hashlib.sha256();deadline=time.monotonic()+30;count=0
            while block:=stream.read(65536):
                count+=len(block)
                if count>MAX_BYTES or time.monotonic()>deadline:raise ValueError('Snapshot verification exceeds bounds')
                digest.update(block)
            if digest.hexdigest()!=sha or identity(os.fstat(stream.fileno()))!=stamp or identity(path.stat())!=stamp:raise ValueError('Sealed snapshot image changed')
            row={'stream':stream,'identity':stamp,'bytes':info.st_size}
        except BaseException:stream.close();raise
        self.readers[key]=row;return row
    def directory(self,command):return checked(self.owner.root/'snapshots'/hashlib.sha256(token(command).encode()).hexdigest())
    def receipt(self,command):
        path=checked(self.directory(command)/'receipt.json')
        if not path.exists():return None
        info=path.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size>32768:raise ValueError('Snapshot receipt exceeds bound')
        value=json.loads(path.read_bytes())
        if not isinstance(value,dict) or value.get('commandId')!=command:raise ValueError('Snapshot identity differs')
        return value
    def assert_held(self,context):
        o=self.owner
        if context.get('purpose')!='recovery' or o.snapshot_live!=context or o.intake.fence!=context or o.intake.calls or o.intake.background or o.schedules.pending or o.observations.tasks:
            raise ValueError('Exact fresh held recovery writer fence required')
    def capture(self,args):
        if set(args)!={'context','commandId','privateContentReviewed'} or args['privateContentReviewed'] is not True:raise ValueError('Explicit private-content-reviewed recovery snapshot required')
        context=args['context'];command=token(args['commandId'])
        signature=hashlib.sha256(json.dumps(args,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        prior=self.receipt(command)
        if prior:
            if prior['signature']!=signature:raise ValueError('Snapshot command differs from original request')
            return {**prior,'replayed':False}
        if not support():raise ValueError('Installed optional library has no snapshot support')
        self.assert_held(context)
        known={'owner.lock','snapshots'}|{name+'.sqlite3'+suffix for name in STORES for suffix in ('','-wal','-shm','-journal')}
        for index,path in enumerate(self.owner.root.iterdir()):
            if index>=64 or path.name not in known:raise ValueError('Unclassified Operations storage; original preserved')
        directory=self.directory(command);directory.mkdir(mode=0o700,parents=True,exist_ok=False)
        receipt={'commandId':command,'signature':signature,'status':'unknown','replayed':False,'activationProvided':False,'coverage':'owner-sqlite-stores','completeProductBackup':False}
        write(directory/'receipt.json',receipt)
        from amplifier_operations.snapshots import capture_snapshot
        images=capture_snapshot({name:self.owner.root/(name+'.sqlite3') for name in STORES},directory/'images',assert_held=lambda:self.assert_held(context),max_bytes=MAX_BYTES)
        receipt={**receipt,'status':'captured','snapshot':images,'omissions':[{'kind':'canonical-native-history-and-external-process-authority','owner':'native ACP'},{'kind':'retained-snapshot-artifacts','owner':'product application-tree archive'}]}
        write(directory/'receipt.json',receipt);return receipt
    def read(self,args):
        if set(args)!={'commandId','store','offset','limit'} or args['store'] not in (*STORES,'manifest'):raise ValueError('Exact bounded snapshot window required')
        offset=args['offset'];limit=args['limit']
        if type(offset) is not int or offset<0 or type(limit) is not int or not 1<=limit<=65536:raise ValueError('Snapshot byte window must be 1..65536')
        if self.owner.closed:raise ValueError('Snapshot owner is closed')
        receipt=self.receipt(args['commandId'])
        if not receipt or receipt['status']!='captured':raise ValueError('Original snapshot is not sealed; do not replay capture')
        directory=self.directory(args['commandId'])/'images'
        if args['store']=='manifest':name='manifest.json';sha=receipt['snapshot']['manifestSha256']
        else:
            row=next((row for row in receipt['snapshot']['stores'] if row['id']==args['store']),None)
            if not row or row['status']!='captured':raise ValueError('Selected original store was absent')
            name=row['file'];sha=row['sha256']
        path=checked(directory/name)
        # Only a selected sealed descriptor is retained, never image content.
        # Its original receipt/manifest identity is part of the bounded LRU key.
        key=(args['commandId'],args['store'],receipt['signature'],receipt['snapshot']['manifestSha256'],sha)
        row=self.reader(key,path,sha);stream=row['stream'];size=row['bytes']
        if offset>size:raise ValueError('Snapshot offset exceeds selected image')
        try:
            stream.seek(offset);raw=stream.read(limit)
            if identity(os.fstat(stream.fileno()))!=row['identity'] or identity(path.stat())!=row['identity']:raise ValueError('Sealed snapshot image changed')
        except BaseException:
            self.readers.pop(key,None);stream.close();raise
        return {'data':base64.b64encode(raw).decode(),'encoding':'base64','bytes':size,'sha256':sha,'offset':offset,'nextOffset':offset+len(raw) if offset+len(raw)<size else None,'replayed':False}

def restore_inactive(snapshot,destination,*,manifest_sha256,assert_held):
    """Caller authenticates origin/review and owns a NEW inactive destination.

    A SHA binds reviewed bytes; it is not authentication or activation authority.
    The existing product archive/recovery coordinator owns those decisions.
    """
    from amplifier_operations.snapshots import restore_snapshot
    if not callable(assert_held):raise ValueError('Caller-held restore guard required')
    assert_held()
    manifest=checked(Path(snapshot)/'manifest.json')
    # Nonblocking open plus descriptor validation avoids trusting a path stat:
    # a FIFO or a file replaced between inspection and open must never block.
    flags=os.O_RDONLY|getattr(os,'O_NONBLOCK',0)|getattr(os,'O_NOFOLLOW',0)
    fd=os.open(manifest,flags)
    with os.fdopen(fd,'rb') as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size>16384:raise ValueError('Bounded regular Operations manifest required')
        raw=stream.read(16385)
        assert_held()
        after=os.fstat(stream.fileno())
        if len(raw)>16384 or any(getattr(after,key)!=getattr(info,key) for key in ('st_dev','st_ino','st_mode','st_nlink','st_size','st_mtime_ns','st_ctime_ns')):raise ValueError('Bounded unchanged Operations manifest required')
    value=json.loads(raw)
    if not isinstance(value,dict) or not isinstance(value.get('stores'),list) or len(value['stores'])!=len(STORES) or any(not isinstance(row,dict) or not isinstance(row.get('id'),str) for row in value['stores']) or {row['id'] for row in value['stores']}!=set(STORES):raise ValueError('Exact Operations store census required')
    result=restore_snapshot(snapshot,destination,manifest_sha256=manifest_sha256,assert_held=assert_held,max_bytes=MAX_BYTES)
    return {**result,'readyForActivation':False,'completeProductRestore':False}
