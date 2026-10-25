#!/usr/bin/env python3
"""Dedicated local kind workspace and reproducible worker-pod recovery proof.

No default kubeconfig, cloud resources, secret output, deletion, or pruning.
"""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / '.scratch/k8s'
KIND = ROOT / '.scratch/tools/kind'
BINARY = ROOT / '.scratch/processor-cluster'
NAME = 'dataset-processor-demo'
CONTEXT = 'kind-' + NAME
NAMESPACE = 'dataset-processor'
NODE = 'kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5'
KIND_SHA = '0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f'

def command(args, data=None, timeout=300):
    result = subprocess.run([str(x) for x in args], input=data, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, cwd=ROOT, timeout=timeout)
    if result.returncode:
        # Never echo arbitrary command output: some commands handle Secret objects.
        raise RuntimeError('Command failed: ' + str(args[0]) + ' ' + str(args[1]) + '; inspect local component diagnostics')
    return result.stdout

def kube(*args, data=None, timeout=300):
    return command(['kubectl', '--kubeconfig', STATE/'kubeconfig', '--context', CONTEXT,
                    '--namespace', NAMESPACE, *args], data, timeout)

def apply(obj):
    return kube('apply', '-f', '-', data=json.dumps(obj).encode())

def wait_for(fn, seconds=180):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = fn()
        if value:
            return value
        time.sleep(2)
    raise RuntimeError('Acceptance deadline exceeded; evidence was not marked complete')

def check_context():
    if not (STATE/'kubeconfig').is_file():
        raise RuntimeError('Dedicated kubeconfig missing; run up first')
    contexts = json.loads(command(['kubectl','--kubeconfig',STATE/'kubeconfig','config','view','-o','json']))
    if {v['name'] for v in contexts['contexts']} != {CONTEXT}:
        raise RuntimeError('Refusing kubeconfig containing unrelated contexts')
    if NAME not in command([KIND,'get','clusters']).decode().splitlines():
        raise RuntimeError('Dedicated local kind cluster missing')

def secret(name, names, digest):
    exists = kube('get','secret','--ignore-not-found',name,'-o','name')
    if exists:
        actual = kube('get','secret',name,'-o','jsonpath={.metadata.annotations.processor-local-bundle}').decode()
        if actual != digest:
            raise RuntimeError('Existing secret has different bootstrap identity; refusing rotation')
        return
    obj = json.loads(kube('create','secret','generic',name,
                        *['--from-file='+n+'='+str(STATE/'private'/n) for n in names],
                        '--dry-run=client','-o','json'))
    obj['immutable'] = True
    obj['metadata']['annotations'] = {'processor-local-bundle': digest}
    apply(obj)

def manifest(image, port, run_id, phase):
    return json.loads(command([BINARY,'cluster','manifest','--image',image,'--port',port,
                              '--run-id',run_id,'--phase',phase]))

def up(port):
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    KIND.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not KIND.exists():
        import platform
        if (platform.system(), platform.machine()) != ('Darwin','arm64'):
            raise RuntimeError('Install kind v0.33.0 for your platform under .scratch/tools/kind')
        data = urllib.request.urlopen('https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-darwin-arm64').read()
        if hashlib.sha256(data).hexdigest() != KIND_SHA:
            raise RuntimeError('kind checksum mismatch')
        KIND.write_bytes(data)
        KIND.chmod(0o700)
    clusters = command([KIND,'get','clusters']).decode().splitlines()
    if NAME not in clusters:
        print('Creating dedicated local kind cluster.', flush=True)
        command([KIND,'create','cluster','--name',NAME,'--image',NODE,'--kubeconfig',STATE/'kubeconfig','--wait','120s'],timeout=600)
    check_context()
    (STATE/'kubeconfig').chmod(0o600)
    command(['go','build','-o',BINARY,'./cmd/processor'])
    command([BINARY,'cluster','init','--output',STATE/'private'])
    digest = command([BINARY,'cluster','digest','--directory',STATE/'private']).decode().strip()
    print('Building and loading the application image.', flush=True)
    command(['docker','build','-t','dataset-processor:k8s-build','.'],timeout=600)
    image_id = command(['docker','image','inspect','dataset-processor:k8s-build','--format','{{.Id}}']).decode().strip()
    image = 'dataset-processor:k8s-' + image_id.removeprefix('sha256:')[:12]
    command(['docker','tag','dataset-processor:k8s-build',image])
    command([KIND,'load','docker-image','--name',NAME,image],timeout=600)
    run_id = str(int(time.time()))
    dependencies = manifest(image,port,run_id,'dependencies')
    apply(dependencies['items'][0])
    secret('processor-owner',['owner','password','pgpass','ca.crt'],digest)
    secret('processor-database',['server.crt','server.key','db-password'],digest)
    apply(dependencies)
    print('Waiting for PostgreSQL and local S3/SQS emulation.', flush=True)
    kube('rollout','status','statefulset/postgres','--timeout=240s')
    kube('rollout','status','deployment/moto','--timeout=240s')
    apply(manifest(image,port,run_id,'bootstrap'))
    kube('wait','--for=condition=complete','job/processor-init-'+run_id,'--timeout=180s')
    apply(manifest(image,port,run_id,'roles'))
    for role in ['api','coordinator','worker']:
        kube('rollout','status','deployment/processor-'+role,'--timeout=180s')
    (STATE/'workspace.json').write_text(json.dumps({'image':image,'image_id':image_id,'port':port,'context':CONTEXT},indent=2)+'\n')
    print('Local Kubernetes roles ready. Password file: '+str(STATE/'private/password'))
    print('Open the workspace: python3 scripts/kubernetes.py forward')

def role_args(role,args):
    patch = {'spec':{'template':{'spec':{'containers':[{'name':'processor','args':[role,'--profile','cluster',*args]}]}}}}
    kube('patch','deployment/processor-'+role,'--type=strategic','-p',json.dumps(patch))
    kube('rollout','status','deployment/processor-'+role,'--timeout=180s')

def execute(*args):
    return kube('exec','deployment/processor-api','--','/processor',*args)

def inspect(job):
    return json.loads(execute('job','inspect','--profile','cluster','--job',job,'--summary'))

def pod(role):
    values=json.loads(kube('get','pods','-l','app=processor-'+role,'-o','json'))['items']
    running=[v for v in values if not v['metadata'].get('deletionTimestamp') and v['status'].get('phase')=='Running']
    if len(running)!=1: return None
    v=running[0]
    return {'uid':v['metadata']['uid'],'name':v['metadata']['name'],'image_id':v['status']['containerStatuses'][0].get('imageID')}

def recover():
    check_context()
    evidence_dir=STATE/('recovery-'+str(time.time_ns()))
    evidence_dir.mkdir(mode=0o700)
    # This dedicated demonstration temporarily pauses only its owned worker.
    kube('scale','deployment/processor-worker','--replicas=0')
    wait_for(lambda: not json.loads(kube('get','pods','-l','app=processor-worker','-o','json'))['items'])
    worker_modified=False
    try:
        role_args('coordinator',['--chunk-bytes','80'])
        submitted=json.loads(execute('job','submit','--profile','cluster','--input','/testdata/transactions.csv','--key','pod-recovery-'+str(time.time_ns())))
        job=submitted['id']
        prepared=wait_for(lambda: (v if v['parts']>=2 and v['state']=='processing' else None) if (v:=inspect(job)) else None)
        worker_modified=True
        role_args('worker',['--job',job,'--checkpoint','.scratch/claimed.json','--lease','2s'])
        kube('scale','deployment/processor-worker','--replicas=1')
        kube('rollout','status','deployment/processor-worker','--timeout=180s')
        claimed=wait_for(lambda: (v if v['chunks'] and v['chunks'][0]['attempts']==1 else None) if (v:=inspect(job)) else None)
        old=wait_for(lambda:pod('worker'))
        print('Worker claimed chunk zero; replacing its pod with the normal worker.',flush=True)
        role_args('worker',[])
        worker_modified=False
        replacement=wait_for(lambda: (v if v['uid']!=old['uid'] else None) if (v:=pod('worker')) else None)
        final=wait_for(lambda: (v if v['state'] in ['succeeded','failed','cancelled'] else None) if (v:=inspect(job)) else None,240)
        assert final['state']=='succeeded','Recovered job did not succeed'
        assert (final['records'],final['accepted'],final['rejected'])==(6,3,3),'Unexpected record counts'
        assert final['committed_chunks']==final['parts'],'Missing selected output chunks'
        assert final['chunks'][0]['attempts']==2,'No abandoned-lease takeover proved'
        assert final['input_version']==prepared['input_version'] and final['input_sha256']==prepared['input_sha256'],'Input identity changed'
        artifacts={}
        for name in ['clean.csv','rejected.csv','manifest.json']:
            data=execute('job','fetch','--profile','cluster','--job',job,'--name',name)
            (evidence_dir/name).write_bytes(data)
            if name!='manifest.json':
                assert data==(ROOT/'testdata/expected'/name).read_bytes(),'Output bytes differ from independent fixture'
            artifacts[name]=hashlib.sha256(data).hexdigest()
        # Coordinator and API replacement must also preserve the completed job.
        coordinator_before=wait_for(lambda:pod('coordinator'))
        role_args('coordinator',[])
        coordinator_after=wait_for(lambda:pod('coordinator'))
        api_before=wait_for(lambda:pod('api'))
        kube('rollout','restart','deployment/processor-api')
        kube('rollout','status','deployment/processor-api','--timeout=180s')
        api_after=wait_for(lambda:pod('api'))
        assert coordinator_before['uid']!=coordinator_after['uid'] and api_before['uid']!=api_after['uid']
        assert inspect(job)==final,'Published job changed after role replacements'
        for name in ['clean.csv','rejected.csv','manifest.json']:
            assert hashlib.sha256(execute('job','fetch','--profile','cluster','--job',job,'--name',name)).hexdigest()==artifacts[name]
        report={'status':'passed','context':CONTEXT,'job':final,'worker_before':old,'worker_after':replacement,'coordinator_before':coordinator_before,'coordinator_after':coordinator_after,'api_before':api_before,'api_after':api_after,'artifacts_sha256':artifacts,'limits':'PostgreSQL and Moto remained running; emulator restart durability and AWS are not tested.'}
    finally:
        if worker_modified: role_args('worker',[])
        kube('scale','deployment/processor-worker','--replicas=1')
        role_args('coordinator',[])
    (evidence_dir/'evidence.json').write_text(json.dumps(report,indent=2)+'\n')
    print('Pod recovery acceptance passed. Evidence: '+str(evidence_dir/'evidence.json'))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['up','recovery','forward'])
    parser.add_argument('--port',type=int,default=8089)
    args=parser.parse_args()
    try:
        if args.action in ['up','recovery']:
            STATE.mkdir(parents=True,exist_ok=True,mode=0o700)
            with (STATE/'operation.lock').open('a') as lock:
                try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError: raise RuntimeError('Another local cluster operation is running')
                if args.action=='up':up(args.port)
                else:recover()
        else:
            check_context()
            port=json.loads((STATE/'workspace.json').read_text())['port']
            while True:
                result=subprocess.run(['kubectl','--kubeconfig',str(STATE/'kubeconfig'),'--context',CONTEXT,'--namespace',NAMESPACE,'port-forward','--address','127.0.0.1','deployment/processor-api',f'{port}:{port}'],stderr=subprocess.PIPE)
                if b'address already in use' in result.stderr:
                    raise RuntimeError('Local port is occupied; choose a free port with up --port PORT')
                print('Reconnecting local port-forward to the current API pod.',flush=True)
                time.sleep(2)
    except KeyboardInterrupt:
        sys.exit(0)
    except (RuntimeError,AssertionError,subprocess.TimeoutExpired) as exc:
        print(str(exc),file=sys.stderr)
        sys.exit(1)
