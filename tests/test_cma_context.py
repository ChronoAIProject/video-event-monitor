import unittest
from copy import deepcopy
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from cma_context import ContextCleanup


class NativeCma:
    """Native asynchronous lifecycle with maintenance advancing the projection."""
    def __init__(self):
        self.aid = 'agt_example123'
        self.state = 'ready'
        self.workspace = 'wks_example123'
        self.profile = 'agp_example123'
        self.terminal_at = '1970-01-01T00:10:00Z'
        self.turns = [{'turn_sequence': 3, 'outcome': {
            'type': 'completed', 'at': self.terminal_at}}]
        self.queue = {'agent_id': self.aid, 'data': [], 'next_after': None}
        self.seq = 349
        self.mutations = []
        self.calls = []
        self.next_cursor = None

    def call(self, base, path, method='GET'):
        self.calls.append((base, path, method))
        if method != 'GET':
            self.mutations.append((path, method))
            return None  # 202 schedules work; status has NOT changed yet.
        if path.startswith('agents?'):
            state = parse_qs(urlsplit(path).query)['status'][0]
            items = [] if self.state != state else [{
                'id': self.aid, 'workspace_id': self.workspace,
                'profile': {'id': self.profile}, 'status': self.state,
                'updated_at': '1970-01-01T00:16:40Z'}]
            return {'items': items, 'next_cursor': self.next_cursor}
        if path.endswith('/queued-inputs'):
            assert base == 'https://example.invalid/proxy/cma/api/v2'
            return deepcopy(self.queue)
        if path.endswith('/transcript'):
            self.seq += 1  # Background events are not conversation activity.
            return {'agent_id': self.aid, 'through_seq': self.seq, 'turns': deepcopy(self.turns)}
        return {'id': self.aid, 'workspace': {'id': self.workspace},
                'profile': {'id': self.profile}, 'status': self.state,
                'updated_at': '1970-01-01T00:16:40Z'}


class ContextCleanupTests(unittest.TestCase):
    def setup_cleanup(self):
        cfg = {'cma_url': 'https://example.invalid/proxy/cma/api/v1',
               'agent_profile_ids': ['agp_example123'],
               'workspace_id': 'wks_example123', 'grace_seconds': 300}
        obj = ContextCleanup(cfg, 'synthetic-owner-credential-for-test-only')
        fake = NativeCma()
        obj.call = fake.call
        return obj, fake

    def test_terminal_clock_reclaims_old_turn_despite_recent_maintenance(self):
        obj, fake = self.setup_cleanup()
        status = obj.run(now=1000)
        self.assertIsNone(status['error'])
        self.assertEqual(fake.mutations, [('agents/' + fake.aid + '/stop', 'POST')])
        self.assertEqual(status['retired'], 0)
        self.assertEqual(status['inflight'], 1)

    def test_async_stop_and_delete_are_polled_once_until_deleted(self):
        obj, fake = self.setup_cleanup()
        obj.run(now=1000)
        obj.run(now=1001)  # Still ready while Stop is pending.
        self.assertEqual(len(fake.mutations), 1)
        fake.state = 'stopped'
        obj.run(now=1002)
        self.assertEqual(fake.mutations[-1], ('agents/' + fake.aid, 'DELETE'))
        obj.run(now=1003)  # Worktree removed, but no deleted publication yet.
        self.assertEqual(len(fake.mutations), 2)
        self.assertEqual(obj.status['retired'], 0)
        fake.state = 'deleted'
        self.assertEqual(obj.run(now=1004)['retired'], 1)
        self.assertEqual(obj.status['pending'], 0)
        self.assertEqual(obj.status['inflight'], 0)
        obj.run(now=1005)
        self.assertEqual(len(fake.mutations), 2)
        self.assertFalse(any('sandboxes/' in p or 'workspaces/' in p for _, p, _ in fake.calls))

    def test_stopped_agent_still_requires_terminal_age_and_empty_queue(self):
        obj, fake = self.setup_cleanup()
        fake.state = 'stopped'
        obj.run(now=700)
        self.assertEqual(fake.mutations, [])
        obj.run(now=1000)
        self.assertEqual(fake.mutations, [('agents/' + fake.aid, 'DELETE')])

    def test_working_approval_unfinished_and_recent_turns_are_preserved(self):
        for state, outcome in [('working', 'completed'), ('waiting_approval', 'completed'),
                               ('ready', 'active'), ('ready', 'unknown')]:
            with self.subTest(state=state, outcome=outcome):
                obj, fake = self.setup_cleanup()
                fake.state = state
                fake.turns[0]['outcome']['type'] = outcome
                obj.run(now=1000)
                self.assertFalse(fake.mutations)
        obj, fake = self.setup_cleanup()
        obj.run(now=700)
        self.assertFalse(fake.mutations)
        fake.turns = []
        obj.run(now=1000)
        self.assertFalse(fake.mutations)

    def test_v2_queue_data_cursor_or_missing_evidence_preserves_work(self):
        for patch in [{'data': [{'text': 'pending'}]}, {'next_after': 'more'},
                      {'data': None}, {'agent_id': 'agt_other12345'}]:
            with self.subTest(patch=patch):
                obj, fake = self.setup_cleanup()
                fake.queue.update(patch)
                obj.run(now=1000)
                self.assertFalse(fake.mutations)

    def test_other_profiles_and_workspace_are_preserved(self):
        for field in ['workspace', 'profile']:
            obj, fake = self.setup_cleanup()
            setattr(fake, field, 'other_resource123')
            obj.run(now=1000)
            self.assertFalse(fake.mutations)

    def test_new_input_between_reads_cancels_cleanup(self):
        obj, fake = self.setup_cleanup()
        def call(base, path, method='GET'):
            result = fake.call(base, path, method)
            if path.endswith('/transcript') and fake.seq >= 351:
                result['turns'].append({'turn_sequence': 4, 'outcome': {'type': 'active'}})
            return result
        obj.call = call
        obj.run(now=1000)
        self.assertFalse(fake.mutations)

    def test_deleted_tombstones_are_filtered_and_more_work_is_reported(self):
        obj, fake = self.setup_cleanup()
        fake.next_cursor = 'next-page'
        obj.run(now=1000)
        states = [parse_qs(urlsplit(p).query)['status'][0]
                  for _, p, _ in fake.calls if p.startswith('agents?')]
        self.assertEqual(states, ['ready', 'stopped', 'failed'])
        self.assertTrue(obj.status['scan_limited'])

    def test_uncertain_mutation_is_retained_but_definite_rejection_reports_error(self):
        for error, inflight in [(TimeoutError('private'), 1),
                               (HTTPError('private', 404, 'private', {}, None), 0)]:
            with self.subTest(error=type(error).__name__):
                obj, fake = self.setup_cleanup()
                def call(base, path, method='GET'):
                    if method != 'GET':
                        raise error
                    return fake.call(base, path, method)
                obj.call = call
                result = obj.run(now=1000)
                self.assertEqual(result['inflight'], inflight)
                self.assertEqual(result['retired'], 0)
                self.assertNotIn('private', str(result))
                self.assertIsNotNone(result['error'])

    def test_urls_ids_and_retention_are_bounded(self):
        obj, _ = self.setup_cleanup()
        for patch in [{'cma_url': 'http://example.invalid/api/v1'},
                      {'cma_url': 'https://user:private@example.invalid/api/v1'},
                      {'cma_url': 'https://example.invalid/not-an-api'},
                      {'workspace_id': '../other'}, {'grace_seconds': 0},
                      {'agent_profile_ids': ['../invalid']}]:
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                ContextCleanup(obj.config | patch, obj.token)
