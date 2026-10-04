"""Trusted held Operations-store images; not a product archive or activation API."""
import base64,hashlib,json,os,time,stat
from pathlib import Path

STORES=('intake','schedules','operations','questions','observations','transfer','legacy')
MAX_BYTES=64*1024*1024

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
    def __init__(self,owner):self.owner=owner
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
        receipt=self.receipt(args['commandId'])
        if not receipt or receipt['status']!='captured':raise ValueError('Original snapshot is not sealed; do not replay capture')
        directory=self.directory(args['commandId'])/'images'
        if args['store']=='manifest':name='manifest.json';sha=receipt['snapshot']['manifestSha256']
        else:
            row=next((row for row in receipt['snapshot']['stores'] if row['id']==args['store']),None)
            if not row or row['status']!='captured':raise ValueError('Selected original store was absent')
            name=row['file'];sha=row['sha256']
        path=checked(directory/name)
        # Sealed artifact files are immutable; each selected read verifies its
        # bounded image hash without hydrating any record or unrelated store.
        before=path.stat();size=before.st_size
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or size>MAX_BYTES:raise ValueError('Sealed snapshot image exceeds bounds')
        digest=hashlib.sha256();deadline=time.monotonic()+30;count=0
        with path.open('rb') as stream:
            while block:=stream.read(65536):
                count+=len(block)
                if count>MAX_BYTES or time.monotonic()>deadline:raise ValueError('Snapshot verification exceeds bounds')
                digest.update(block)
            if digest.hexdigest()!=sha or path.stat()!=before:raise ValueError('Sealed snapshot image changed')
            if offset>size:raise ValueError('Snapshot offset exceeds selected image')
            stream.seek(offset);raw=stream.read(limit)
            if path.stat()!=before:raise ValueError('Sealed snapshot image changed')
        return {'data':base64.b64encode(raw).decode(),'encoding':'base64','bytes':size,'sha256':sha,'offset':offset,'nextOffset':offset+len(raw) if offset+len(raw)<size else None,'replayed':False}

def restore_inactive(snapshot,destination,*,manifest_sha256,assert_held):
    """Caller authenticates origin/review and owns a NEW inactive destination.

    A SHA binds reviewed bytes; it is not authentication or activation authority.
    The existing product archive/recovery coordinator owns those decisions.
    """
    from amplifier_operations.snapshots import restore_snapshot
    manifest=checked(Path(snapshot)/'manifest.json')
    if manifest.stat().st_size>16384:raise ValueError('Bounded Operations manifest required')
    value=json.loads(manifest.read_bytes())
    if not isinstance(value,dict) or not isinstance(value.get('stores'),list) or len(value['stores'])!=len(STORES) or any(not isinstance(row,dict) or not isinstance(row.get('id'),str) for row in value['stores']) or {row['id'] for row in value['stores']}!=set(STORES):raise ValueError('Exact Operations store census required')
    result=restore_snapshot(snapshot,destination,manifest_sha256=manifest_sha256,assert_held=assert_held,max_bytes=MAX_BYTES)
    return {**result,'readyForActivation':False,'completeProductRestore':False}
