import asyncio

import pytest

from amplifier_unified_operations.quiescence import Intake
from amplifier_unified_operations.server import Owner
from amplifier_unified_operations.migrate import import_page


CONTEXT = dict(fenceId='fence-one', commandId='command-one', purpose='recovery',
               instanceId='host-one', dataScope='private-installation')
SID = 'ahp-session:/selected'


def proof(**overrides):
    return dict(verified=True, fenceId=CONTEXT['fenceId'], commandId=CONTEXT['commandId'],
                outcome='unchanged', instanceId=CONTEXT['instanceId'],
                dataScope=CONTEXT['dataScope'], receiptId='verified-receipt', **overrides)


async def test_active_admission_cannot_race_the_fence_and_reads_remain_available(tmp_path):
    entered, finish = asyncio.Event(), asyncio.Event()

    async def host(method, args):
        if method == 'nativeControl':
            entered.set()
            await finish.wait()
            return {'success': True, 'output': {'process_id': 'owned-process'}}
        if method == 'operationSources':
            return []
        raise AssertionError(method)

    events=[]
    async def changed(method,args):
        events.append(method)

    owner = Owner({'dataDir': str(tmp_path)}, host, changed)
    params = dict(session=SID, operation='operations.submit', origin='ui',
                  args={'requestId': 'submit-one', 'command': 'fixture'})
    active = asyncio.create_task(owner.request('action', params))
    try:
        await entered.wait()
        assert (await owner.request('quiescence.acquire', CONTEXT))['acquired'] is False
        finish.set()
        await active
        assert events.count('owner/idle')==1
        assert (await owner.request('quiescence.acquire', CONTEXT))['acquired'] is True
        with pytest.raises(ValueError, match='intake is closed'):
            await owner.request('action', {**params, 'args': {**params['args'], 'requestId': 'submit-two'}})
        receipt = await owner.request('action', dict(session=SID, operation='operations.request', args={'requestId': 'submit-one'}))
        assert receipt['operationId'] == 'owned-process'
        assert (await owner.request('quiescence.release', {**CONTEXT, 'outcome': 'unknown'}))['released'] is False
        with pytest.raises(ValueError, match='proof'):
            await owner.request('quiescence.release', {**CONTEXT, 'outcome': 'unchanged', 'proof': {**proof(), 'dataScope': 'other'}})
        await owner.request('quiescence.release', {**CONTEXT, 'outcome': 'unchanged', 'proof': proof()})
    finally:
        finish.set()
        await asyncio.gather(active, return_exceptions=True)
        await owner.close()


async def test_retained_fence_blocks_background_and_offline_import_across_restart(tmp_path):
    async def forbidden(*args):
        raise AssertionError('No background or host callback may run under the retained fence')

    root = tmp_path / 'owner'
    owner = Owner({'dataDir': str(root)}, forbidden, forbidden)
    await owner.request('quiescence.acquire', CONTEXT)
    await owner.close()
    source = tmp_path / 'legacy.sqlite3'
    import sqlite3
    with sqlite3.connect(source):
        pass
    with pytest.raises(ValueError, match='retained quiescence'):
        import_page(source, root, source_session='old', session=SID, kind='schedule')
    owner = Owner({'dataDir': str(root)}, forbidden, forbidden)
    owner.schedules.tick = forbidden
    owner.observations.tick = forbidden
    try:
        await owner.request('start', {'observations': True})
        await asyncio.sleep(0.02)
        assert not owner.tick_task.done()
        assert (await owner.request('quiescence.inspect', {}))['intakeClosed'] is True
        assert (await owner.request('action', dict(session=SID, operation='schedule.request', commandId='inspect', args={'requestId': 'old'})))['receipt'] is None
        with pytest.raises(ValueError, match='current fence'):
            await owner.request('quiescence.release', {**CONTEXT, 'commandId': 'other', 'outcome': 'unchanged', 'proof': proof()})
        await owner.request('quiescence.release', {**CONTEXT, 'outcome': 'unchanged', 'proof': proof()})
    finally:
        await owner.close()


def test_pending_work_and_exact_release_receipts(tmp_path):
    intake = Intake(tmp_path / 'intake.sqlite3')
    try:
        assert intake.acquire(CONTEXT, pending=1)['acquired'] is False
        intake.background = 1
        assert intake.acquire(CONTEXT)['acquired'] is False
        intake.background = 0
        assert intake.acquire(CONTEXT)['acquired'] is True
        assert intake.acquire(CONTEXT)['acquired'] is True
        release = {**CONTEXT, 'outcome': 'unchanged', 'proof': {'kind': 'admission-refused'}}
        assert intake.release(release)['released'] is True
        assert intake.release(release)['released'] is True
        with pytest.raises(ValueError, match='exact retained'):
            intake.release({**release, 'outcome': 'unknown'})
        with pytest.raises(ValueError, match='released fence'):
            intake.acquire(CONTEXT)
    finally:
        intake.close()
