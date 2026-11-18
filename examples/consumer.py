#!/usr/bin/env python3
"""Downstream consumer and source retention verification.

Run after building both binaries:
  go build -o bin/laminad ./cmd/laminad
  go build -o bin/lamina-consumer ./cmd/lamina-consumer
  python3 examples/consumer.py --workflow all

Every scenario starts real source and consumer processes on temporary
directories. Nothing is mocked: the consumer only sees the public HTTP feed.
"""
import argparse, concurrent.futures, json, pathlib, shutil, socket, subprocess, tempfile, time, urllib.error, urllib.request

WORKFLOWS = ['consumer-catch-up', 'downstream-disconnect', 'downstream-duplicate',
             'downstream-retention', 'downstream-rebuild', 'downstream-restore-lineage']
ALIASES = {
    'catch-up': 'consumer-catch-up',
    'disconnect': 'downstream-disconnect',
    'duplicate-page': 'downstream-duplicate',
    'retention': 'downstream-retention',
    'rebuild': 'downstream-rebuild',
    'restore-lineage': 'downstream-restore-lineage',
}


class Session:
    def __init__(self, binary, root, extra=None):
        self.binary = str(binary); self.root = pathlib.Path(root); self.extra = extra or []
        self.process = None; self.log = None; self.url = None

    def start(self, args=None):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        self.url = f'http://127.0.0.1:{port}/v1/lamina'
        argv = [self.binary, '-data', str(self.root), '-listen', f'127.0.0.1:{port}', *self.extra, *(args or [])]
        self.log = tempfile.TemporaryFile(); self.process = subprocess.Popen(argv, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log.seek(0); raise RuntimeError(self.log.read().decode())
            try:
                self.call('GET', '/health'); return self
            except (OSError, urllib.error.URLError):
                time.sleep(.03)
        raise RuntimeError('service did not become ready')

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try: self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.kill(); self.process.wait()
        if self.log: self.log.close(); self.log = None

    def call(self, method, path, data=None, status=200, raw=False):
        encoded = None if data is None else json.dumps(data, ensure_ascii=False).encode()
        req = urllib.request.Request(self.url + path, data=encoded, method=method,
                                     headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                code = response.status; body = response.read()
        except urllib.error.HTTPError as e:
            code = e.code; body = e.read()
        if code != status:
            raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {body.decode()}')
        return body if raw else json.loads(body)

    # source helpers
    def begin(self, readonly=False): return self.call('POST', '/transactions', {'read_only': readonly}, 201)['id']
    def put(self, tx, row): return self.call('POST', f'/transactions/{tx}/put', {'relation': 'spectra', 'row': row})
    def commit(self, tx, status=200): return self.call('POST', f'/transactions/{tx}/commit', status)
    def query(self, **kwargs): return self.call('POST', '/query', {'relation': 'spectra', **kwargs})
    def stats(self): return self.call('GET', '/stats')
    def changes(self, after=0, limit=100, status=200): return self.call('GET', f'/changes?after={after}&limit={limit}', status=status)
    def ack(self, consumer, watermark, digest=''):
        return self.call('POST', '/changes/ack', {'consumer': consumer, 'watermark': watermark, 'digest': digest})

    # consumer helpers (consumer service also uses /v1/lamina paths)
    def sync(self, status=200, **body):
        return self.call('POST', '/sync', body, status)
    def reinitialize(self, status=200, **body):
        return self.call('POST', '/reinitialize', body, status)
    def status(self): return self.call('GET', '/status')


def require(condition, message):
    if not condition: raise RuntimeError(message)


def schema():
    return {'name': 'spectra', 'primary': 'sample', 'columns': [
        {'name': 'sample', 'type': 'int'},
        {'name': 'band', 'type': 'text'},
        {'name': 'amplitude', 'type': 'int', 'nullable': True},
        {'name': 'valid', 'type': 'bool'}]}


def row(n, amplitude=None):
    return {'sample': n, 'band': f'b{n}', 'amplitude': n * 10 if amplitude is None else amplitude, 'valid': True}


def seed(source):
    source.call('POST', '/relations', schema(), 201)
    tx = source.begin()
    for n in range(1, 4): source.put(tx, row(n))
    return source.commit(tx)['sequence']


def batch(source, start, count):
    tx = source.begin()
    for n in range(start, start + count): source.put(tx, row(n))
    return source.commit(tx)['sequence']


def consumer(root, binary, source_url):
    return Session(binary, root, extra=['-source', source_url]).start()


def rows_of(sink):
    return sink.call('POST', '/query', {'relation': 'spectra'})['rows']


def scenario(name, source_binary, consumer_binary):
    with tempfile.TemporaryDirectory(prefix='lamina-consumer-') as temp:
        temp = pathlib.Path(temp)
        source = Session(source_binary, temp / 'source').start()
        sink = None
        try:
            base = seed(source)
            source_url = source.url
            if name == 'consumer-catch-up':
                sink = consumer(temp / 'sink', consumer_binary, source_url)
                # Bootstrap at the source head, then a lagging consumer catches up
                # across several commits and a page boundary.
                sink.reinitialize()
                require(sink.status()['watermark'] == base, 'bootstrap watermark must equal source head')
                batch(source, 4, 3)
                batch(source, 7, 4)
                b = batch(source, 11, 2)
                result = sink.sync(page_limit=2)
                require(result['caught_up'] and result['watermark'] == b, 'consumer did not catch up to head')
                require(result['pages'] >= 2, 'page limit was not exercised')
                require(rows_of(sink) == source.query()['rows'], 'downstream rows diverge from source')
                # The consumer acknowledged exactly what it applied.
                states = source.call('GET', '/changes?after=%d' % b)['retention']
                require(sink.status()['digest'] == source.changes(after=b)['cursor_digest'],
                        'consumer watermark digest does not match the source lineage')
                # A checkpoint advances the journal base but must not disturb the
                # change archive or the consumer's ability to keep reading.
                checkpoint = source.call('POST', '/checkpoint')['sequence']
                identity = source.call('GET', '/identity')
                require(identity['journal_base'] == checkpoint, 'checkpoint did not rebase the journal')
                require(identity['change_base'] <= base, 'checkpoint moved the retained change history')
                more = batch(source, 20, 1)
                after_checkpoint = sink.sync()
                require(after_checkpoint['watermark'] == more and
                        rows_of(sink) == source.query()['rows'],
                        'consumer could not continue across a checkpoint')
                # Durable consumer registry: the acknowledged watermark is visible
                # to operations and survives the source restart below.
                registry = source.call('GET', '/consumers')['consumers']
                assert any(c['consumer'] == 'default' and c['watermark'] == more for c in registry)
                source.stop(); source.start(); source_url = source.url
                listed = source.call('GET', '/consumers')['consumers']
                require(any(c['consumer'] == 'default' and c['watermark'] == more for c in listed),
                        'consumer acknowledgement did not survive the source restart')
                digest = source.call('GET', '/changes/digest?sequence=%d&digest=%s' %
                                     (more, after_checkpoint['digest']))
                require(digest['match'] and digest['retained'], 'resume digest is not verifiable')
                observations = {'base': base, 'head': more, 'sync': result, 'checkpoint': after_checkpoint,
                                'registry': listed, 'digest': digest}

            elif name == 'downstream-disconnect':
                sink = consumer(temp / 'sink', consumer_binary, source_url)
                sink.reinitialize()
                batch(source, 4, 5)
                head = batch(source, 9, 2)
                partial = sink.sync(max_pages=1, page_limit=1)
                require(not partial['caught_up'] and partial['watermark'] < head, 'partial sync must stop short')
                applied = partial['watermark']
                sink.stop()
                # Source keeps writing while the consumer is down.
                later = batch(source, 11, 3)
                sink = consumer(temp / 'sink', consumer_binary, source_url)
                require(sink.status()['watermark'] == applied, 'restart lost or overstated the watermark')
                resumed = sink.sync(page_limit=2)
                require(resumed['caught_up'] and resumed['watermark'] == later, 'resume did not reach source head')
                require(rows_of(sink) == source.query()['rows'], 'resumed mirror diverges')
                observations = {'stopped_at': applied, 'head_after_restart': later, 'resumed': resumed}

            elif name == 'downstream-duplicate':
                sink = consumer(temp / 'sink', consumer_binary, source_url)
                sink.reinitialize()
                batch(source, 4, 2)
                head = batch(source, 6, 2)
                first = sink.sync(max_pages=1, page_limit=1)
                require(first['watermark'] < head, 'expected a partial page')
                before = rows_of(sink)
                # Deliver the same page again: data must not change, and a replay
                # of an already applied prefix must verify the same lineage.
                replay = sink.sync(replay_from=first['from'], max_pages=1, page_limit=1)
                require(replay['duplicate_records'] >= 1, 'replay did not report duplicate records')
                require(rows_of(sink) == before, 'duplicate page modified data')
                require(sink.status()['watermark'] == first['watermark'], 'duplicate page moved the watermark')
                finish = sink.sync(page_limit=10)
                require(finish['caught_up'] and rows_of(sink) == source.query()['rows'], 'recovery after duplicate failed')
                # A whole transaction is never split: querying at the watermark of
                # the multi-write commit shows all of its writes or none.
                tx = source.begin()
                for n in (40, 41, 42): source.put(tx, row(n))
                split_seq = source.commit(tx)['sequence']
                sink.sync(page_limit=1)
                at = sink.call('POST', '/query', {'relation': 'spectra', 'at': split_seq})
                keys = {r['sample'] for r in at['rows']}
                require({40, 41, 42} <= keys, 'transaction was visibly split at a page boundary')
                observations = {'from': first['from'], 'watermark': first['watermark'],
                                'replay_duplicates': replay['duplicate_records'], 'final': finish['watermark']}

            elif name == 'downstream-retention':
                # A consumer stays behind; the source is not allowed to grow
                # without bound and writes never wait for the consumer.
                source.call('POST', '/retention', {'max_bytes': 8192, 'max_lag_records': 3})
                sink = consumer(temp / 'sink', consumer_binary, source_url)
                sink.reinitialize()
                # A consumer inside its lag window is protected: the source keeps
                # serving writes while the slow consumer is still owed history.
                pinned_sink = consumer(temp / 'pinned', consumer_binary, source_url)
                pinned_sink.reinitialize()
                pinned_seq = pinned_sink.status()['watermark']
                source.ack('pinned', pinned_seq, pinned_sink.status()['digest'])
                batch(source, 60, 1)
                pinned_state = source.call('GET', '/retention')
                require(pinned_state['history_base'] <= pinned_seq,
                        'live consumer history was pruned inside its lag window')
                # The source keeps accepting writes while the archive is under
                # budget pressure; a slow reader never blocks a commit.
                written = batch(source, 61, 3)
                require(written > pinned_seq, 'source writes were blocked by a slow consumer')
                caught = pinned_sink.sync()
                require(caught['caught_up'] and caught['watermark'] == written,
                        'consumer inside its lag window could not catch up')
                require(rows_of(pinned_sink) == source.query()['rows'], 'protected consumer mirror diverges')
                source.ack('pinned', written, caught['digest'])
                pinned_sink.stop()
                head = base
                for n in range(0, 30):
                    head = batch(source, 100 + n * 3, 3)
                state = source.call('GET', '/retention')
                require(state['history_base'] > 0, 'retention did not advance the history base under budget')
                require(state['retained_bytes'] <= 8192 or state['pinned'],
                        'retention exceeded its byte budget without a live pin')
                require(state['head'] == head, 'retention observed a stale head')
                stale = sink.sync(status=409)
                require('reinitialize' in json.dumps(stale).lower(), 'stale consumer was not told to reinitialize')
                # Reinitializing costs a full bootstrap but restores a true watermark.
                fresh = sink.reinitialize()
                require(fresh['watermark'] == source.stats()['sequence'], 'reinitialized watermark is not the source head')
                require(rows_of(sink) == source.query()['rows'], 'reinitialized mirror diverges')
                source.stop(); source.start()
                persisted = source.call('GET', '/retention')
                require(persisted['max_bytes'] == 8192 and persisted['max_lag_records'] == 3,
                        'retention policy did not survive restart')
                source.call('POST', '/retention', {'max_bytes': 0, 'max_lag_records': 0})
                observations = {'retention': state, 'stale_error': stale, 'reinitialized': fresh,
                                'after_restart': persisted}

            elif name == 'downstream-rebuild':
                # A source rebuilt from an old backup yields the same counts but a
                # different epoch. The consumer must not continue on numbers alone.
                sink = consumer(temp / 'sink', consumer_binary, source_url)
                backup = source.call('GET', '/backup', raw=True)
                path = temp / 'old.lmi'; path.write_bytes(backup)
                boot = sink.reinitialize()
                require(boot['source_id'] == source.stats()['source_id'], 'source identity mismatch')
                # Rebuild a separate source from the old backup and write unrelated
                # rows until the sequence numbers overlap the consumer's watermark.
                rebuilt = Session(source_binary, temp / 'rebuilt').start(['-restore', str(path)])
                try:
                    rebuilt_url = rebuilt.url
                    tx = rebuilt.begin()
                    for n in (200, 201, 202): rebuilt.put(tx, row(n))
                    rebuilt.commit(tx)
                    fork = consumer(temp / 'fork', consumer_binary, rebuilt_url)
                    require(fork.reinitialize()['source_id'] == boot['source_id'],
                            'restored source must keep the logical source identity')
                    require(fork.status()['epoch'] != boot['epoch'],
                            'restore must issue a new epoch')
                    moved = fork.sync()
                    require(moved['caught_up'], 'forked consumer could not sync its own source')
                    before = rows_of(sink)
                    # Point the original consumer at the rebuilt source. Identity +
                    # epoch must reject it even though its numbers are plausible.
                    sink.stop()
                    sink = consumer(temp / 'sink', consumer_binary, rebuilt_url)
                    status = sink.status()
                    require(status['stale'] or status['epoch'] != boot['epoch'], 'rebuilt source not detected')
                    error = sink.sync(status=409)
                    require('identity' in json.dumps(error).lower() or 'epoch' in json.dumps(error).lower(),
                            'sync accepted a rebuilt source')
                    require(rows_of(sink) == before, 'rejected sync modified local data')
                    # The only correct recovery is an explicit bootstrap.
                    again = sink.reinitialize()
                    require(again['epoch'] == fork.status()['epoch'], 'reinitialize did not adopt the new epoch')
                    observations = {'original_epoch': boot['epoch'], 'rebuilt_epoch': status['epoch'],
                                    'rejected': error, 'reinitialized': again}
                finally:
                    rebuilt.stop()

            elif name == 'downstream-restore-lineage':
                # Source restores its own backup into the same lineage. Consumers
                # must reinitialize, and old cursors must be refused, not reinterpreted.
                sink = consumer(temp / 'sink', consumer_binary, source_url)
                sink.reinitialize()
                batch(source, 4, 2)
                batch(source, 6, 2)
                sink.sync()
                recorded = sink.status()
                backup = source.call('GET', '/backup', raw=True)
                source.stop()
                path = temp / 'self.lmi'; path.write_bytes(backup)
                source = Session(source_binary, temp / 'source2', extra=['-restore', str(path)]).start()
                identity = source.call('GET', '/identity')
                require(identity['source_id'] == recorded['source_id'], 'self-restore changed source identity')
                require(identity['epoch'] != recorded['epoch'], 'self-restore kept the old epoch')
                # The backup's sequence is before the consumer watermark, so a
                # naive consumer would resume from a future it never applied.
                # Any cursor below the restored base is refused, including one
                # equal to the new head (the consumer's old watermark is ahead).
                stale = source.changes(after=recorded['watermark'] - 1, status=410)
                digit = source.call('GET', f"/changes/digest?sequence={recorded['watermark'] - 1}", status=410)
                require(not digit['retained'], 'old cursor digest still reported retained')
                sink.stop()
                sink = consumer(temp / 'sink', consumer_binary, source.url)
                denied = sink.sync(status=409)
                require('identity' in json.dumps(denied).lower(), 'restored lineage was not rejected')
                rebased = sink.reinitialize()
                require(rebased['watermark'] == identity['sequence'], 'reinitialize did not align to restored point')
                test = sink.sync()
                require(test['watermark'] == rebased['watermark'], 'empty difference cannot advance watermark')
                observations = {'old': recorded, 'restored': identity, 'stale': stale,
                                'refused': denied, 'reinitialized': rebased}
            else:
                raise ValueError(name)
            return {'workflow': name, 'status': 'verified', 'observations': observations}
        finally:
            if sink: sink.stop()
            source.stop()
            try:
                if 'rebuilt' in locals(): rebuilt.stop()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workflow', choices=['all', *WORKFLOWS, *ALIASES], default='all')
    args = parser.parse_args()
    name = ALIASES.get(args.workflow, args.workflow)
    project = pathlib.Path(__file__).resolve().parents[1]
    source_binary = project / 'bin/laminad'
    consumer_binary = project / 'bin/lamina-consumer'
    if not source_binary.exists() or not consumer_binary.exists():
        raise SystemExit('build first: go build -o bin/laminad ./cmd/laminad && go build -o bin/lamina-consumer ./cmd/lamina-consumer')
    names = WORKFLOWS if args.workflow == 'all' else [name]
    for name in names:
        print(json.dumps(scenario(name, source_binary, consumer_binary), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
