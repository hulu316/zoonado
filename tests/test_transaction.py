from mock import Mock
from tornado import testing, concurrent

from zoonado import transaction, protocol, exc


class TransactionTests(testing.AsyncTestCase):

    def test_instantiation(self):
        client = Mock()

        txn = transaction.Transaction(client)

        self.assertEqual(txn.client, client)
        self.assertIsInstance(txn.request, protocol.TransactionRequest)

    def test_accumulates_requests(self):
        txn = transaction.Transaction(Mock())

        txn.check_version("/foo/bar", version=8)
        txn.set_data("/foo/bar", "some data")
        txn.delete("/foo/bazz")

        self.assertIsInstance(
            txn.request.requests[0], protocol.CheckVersionRequest
        )
        self.assertIsInstance(
            txn.request.requests[1], protocol.SetDataRequest
        )
        self.assertIsInstance(
            txn.request.requests[2], protocol.DeleteRequest
        )

    def test_check_version_operation(self):
        client = Mock()

        txn = transaction.Transaction(client)

        txn.check_version("/foo/bar", version=8)

        sub_request = txn.request.requests[-1]

        self.assertIsInstance(sub_request, protocol.CheckVersionRequest)

        self.assertEqual(sub_request.path, client.normalize_path.return_value)
        client.normalize_path.assert_called_once_with("/foo/bar")
        self.assertEqual(sub_request.version, 8)

    def test_create_operation(self):
        client = Mock()
        client.features.create_with_stat = False

        txn = transaction.Transaction(client)

        txn.create("/foo/bar", data="data", ephemeral=True)

        sub_request = txn.request.requests[-1]

        self.assertIsInstance(sub_request, protocol.CreateRequest)

        self.assertEqual(sub_request.path, client.normalize_path.return_value)
        client.normalize_path.assert_called_once_with("/foo/bar")

        self.assertEqual(sub_request.data, "data")

        self.assertEqual(sub_request.acl, client.default_acl)

        self.assertEqual(sub_request.flags, 1)  # just ephemeral flag set

    def test_create_can_use_create_with_stat(self):
        client = Mock()
        client.features.create_with_stat = True

        txn = transaction.Transaction(client)

        txn.create("/foo/bar", data="data", ephemeral=True)

        sub_request = txn.request.requests[-1]

        self.assertIsInstance(sub_request, protocol.Create2Request)

    def test_create_container_when_not_available(self):
        client = Mock()
        client.features.containers = False

        txn = transaction.Transaction(client)

        with self.assertRaises(ValueError):
            txn.create("/foo/bar", data="data", container=True)

    def test_set_data(self):
        client = Mock()

        txn = transaction.Transaction(client)

        txn.set_data("/foo/bar", '{"what": "hey"}', version=7)

        sub_request = txn.request.requests[-1]

        self.assertIsInstance(sub_request, protocol.SetDataRequest)

        self.assertEqual(sub_request.path, client.normalize_path.return_value)
        client.normalize_path.assert_called_once_with("/foo/bar")

        self.assertEqual(sub_request.data, '{"what": "hey"}')

        self.assertEqual(sub_request.version, 7)

    def test_delete(self):
        client = Mock()

        txn = transaction.Transaction(client)

        txn.delete("/foo/bazz", version=3)

        sub_request = txn.request.requests[-1]

        self.assertIsInstance(sub_request, protocol.DeleteRequest)

        self.assertEqual(sub_request.path, client.normalize_path.return_value)
        client.normalize_path.assert_called_once_with("/foo/bazz")

        self.assertEqual(sub_request.version, 3)

    @testing.gen_test
    def test_committing_with_no_operations(self):
        client = Mock()

        txn = transaction.Transaction(client)

        with self.assertRaises(ValueError):
            yield txn.commit()

    @testing.gen_test
    def test_commit_success(self):
        client = Mock()

        def fake_normalize(path):
            return "/normed" + path

        def fake_denormalize(path):
            return "/de" + path

        client.normalize_path.side_effect = fake_normalize
        client.denormalize_path.side_effect = fake_denormalize

        responses = [
            protocol.CreateResponse(path="/foo/bar"),
            protocol.CheckVersionResponse(),
            protocol.SetDataResponse(stat=Mock()),
            protocol.DeleteResponse(),
        ]

        response = Mock(responses=responses)
        f = concurrent.Future()
        f.set_result(response)
        client.send.return_value = f

        txn = transaction.Transaction(client)

        txn.create("/foo/bar", data="bazz")
        txn.check_version("/foo/bazz", version=8)
        txn.set_data("/foo/bazz", "blee")
        txn.delete("/foo/bloo", version=5)

        result = yield txn.commit()

        assert result

        self.assertEqual(result.created, set(["/de/normed/foo/bar"]))
        self.assertEqual(result.checked, set(["/de/normed/foo/bazz"]))
        self.assertEqual(result.updated, set(["/de/normed/foo/bazz"]))
        self.assertEqual(result.deleted, set(["/de/normed/foo/bloo"]))

    @testing.gen_test
    def test_commit_failure(self):
        client = Mock()

        def fake_normalize(path):
            return "/normed" + path

        def fake_denormalize(path):
            return "/de" + path

        client.normalize_path.side_effect = fake_normalize
        client.denormalize_path.side_effect = fake_denormalize

        responses = [
            exc.RuntimeInconsistency(),
            exc.RolledBack(),
            exc.DataInconsistency(),
            exc.RuntimeInconsistency(),
        ]

        response = Mock(responses=responses)
        f = concurrent.Future()
        f.set_result(response)
        client.send.return_value = f

        txn = transaction.Transaction(client)

        txn.create("/foo/bar", data="bazz")
        txn.check_version("/foo/bazz", version=8)
        txn.set_data("/foo/bazz", "blee")
        txn.delete("/foo/bloo", version=5)

        result = yield txn.commit()

        assert not result

        self.assertEqual(result.created, set([]))
        self.assertEqual(result.checked, set([]))
        self.assertEqual(result.updated, set([]))
        self.assertEqual(result.deleted, set([]))
