from mock import patch, Mock
from tornado import testing, concurrent

from zoonado.protocol.acl import ACL
from zoonado import client, protocol, exc, WatchEvent


class ClientTests(testing.AsyncTestCase):

    def future_value(self, value):
        f = concurrent.Future()
        f.set_result(value)
        return f

    def future_error(self, exception):
        f = concurrent.Future()
        f.set_exception(exception)
        return f

    def test_default_acl_is_unrestricted(self):
        c = client.Zoonado("host,host,host")

        self.assertEqual(len(c.default_acl), 1)

        self.assertEqual(
            c.default_acl[0],
            ACL.make(
                scheme="world", id="anyone",
                read=True, write=True, create=True, delete=True, admin=True
            )
        )

    def test_normalize_path_with_leading_slash(self):
        c = client.Zoonado("host1,host2,host3")

        self.assertEqual(c.normalize_path("/foo/bar"), "/foo/bar")

    def test_normalize_path_with_no_slash(self):
        c = client.Zoonado("host1,host2,host3")

        self.assertEqual(c.normalize_path("foo/bar"), "/foo/bar")

    def test_normalize_path_with_extra_slashes(self):
        c = client.Zoonado("host1,host2,host3")

        self.assertEqual(c.normalize_path("foo//bar"), "/foo/bar")

    def test_normalize_path_with_chroot(self):
        c = client.Zoonado("host1,host2,host3", chroot="/bazz")

        self.assertEqual(c.normalize_path("foo//bar"), "/bazz/foo/bar")

    def test_normalize_path_with_chroot_missing_leading_slash(self):
        c = client.Zoonado("host1,host2,host3", chroot="bazz")

        self.assertEqual(c.normalize_path("foo//bar"), "/bazz/foo/bar")

    def test_denormalize_path_without_chroot(self):
        c = client.Zoonado("host1,host2,host3")

        self.assertEqual(c.denormalize_path("/foo/bar"), "/foo/bar")

    def test_denormalize_path_with_chroot(self):
        c = client.Zoonado("host1,host2,host3", chroot="/bazz")

        self.assertEqual(c.denormalize_path("/bazz/foo/bar"), "/foo/bar")

    def test_denormalize_path_with_chroot_mismatch(self):
        c = client.Zoonado("host1,host2,host3", chroot="/bazz")

        self.assertEqual(c.denormalize_path("/foo/bar"), "/foo/bar")

    @patch.object(client, "Session")
    @testing.gen_test
    def test_start_calls_session_start(self, Session):
        Session.return_value.start.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")

        self.assertEqual(c.session, Session.return_value)

        yield c.start()

        c.session.start.assert_called_once_with()

    @patch.object(client.Zoonado, "ensure_path")
    @patch.object(client, "Session")
    @testing.gen_test
    def test_start_ensures_chroot_path(self, Session, ensure_path):
        Session.return_value.start.return_value = self.future_value(None)
        ensure_path.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3", chroot="/foo/bar")

        yield c.start()

        c.ensure_path.assert_called_once_with("/")

    @patch.object(client, "Features")
    @patch.object(client, "Session")
    def test_features_property(self, Session, Features):
        Session.return_value.conn.version_info = (3, 6, 0)

        c = client.Zoonado("host1,host2,host3")

        self.assertEqual(c.features, Features.return_value)
        Features.assert_called_once_with((3, 6, 0))

    @patch.object(client, "Features")
    @patch.object(client, "Session")
    def test_features_when_no_connection(self, Session, Features):
        Session.return_value.conn = None

        c = client.Zoonado("host1,host2,host3")

        self.assertEqual(c.features, Features.return_value)
        Features.assert_called_once_with((0, 0, 0))

    @patch.object(client, "Session")
    @testing.gen_test
    def test_send_passes_to_session_send(self, Session):
        request = Mock()
        response = Mock()
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3")

        actual = yield c.send(request)

        c.session.send.assert_called_once_with(request)

        self.assertTrue(response is actual)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_send_caches_stats_if_present_on_response(self, Session):
        request = Mock(path="/bazz/foo/bar")
        stat = Mock()
        response = Mock(stat=stat)
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3", chroot="/bazz")

        yield c.send(request)

        self.assertEqual(c.stat_cache["/foo/bar"], stat)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_close_calls_session_close(self, Session):
        Session.return_value.close.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")

        yield c.close()

        c.session.close.assert_called_once_with()

    @patch.object(client, "Session")
    @testing.gen_test
    def test_exists_request(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")

        result = yield c.exists("/foo/bar")

        self.assertTrue(result)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.ExistsRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.watch, False)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_exists_with_chroot(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3", chroot="bazz")

        result = yield c.exists("/foo/bar")

        self.assertTrue(result)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.ExistsRequest)
        self.assertEqual(request.path, "/bazz/foo/bar")
        self.assertEqual(request.watch, False)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_exists_request_with_watch(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")

        yield c.exists("/foo/bar", watch=True)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.ExistsRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.watch, True)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_exists_no_node_error(self, Session):
        Session.return_value.send.return_value = self.future_error(
            exc.NoNode()
        )

        c = client.Zoonado("host1,host2,host3")

        result = yield c.exists("/foo/bar", watch=True)

        self.assertFalse(result)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_delete(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")

        yield c.delete("/foo/bar")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.DeleteRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.version, -1)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_delete_with_chroot(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3", chroot="/bar")

        yield c.delete("/foo/bar")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.DeleteRequest)
        self.assertEqual(request.path, "/bar/foo/bar")
        self.assertEqual(request.version, -1)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_delete_with_stat_cache_unforced(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")
        c.stat_cache["/foo/bar"] = Mock(version=33)

        yield c.delete("/foo/bar")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.DeleteRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.version, 33)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_delete_with_stat_cache_forced(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")
        c.stat_cache["/foo/bar"] = Mock(version=33)

        yield c.delete("/foo/bar", force=True)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.DeleteRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.version, -1)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_get_data(self, Session):
        response = Mock(data=u"wooo")
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3")

        result = yield c.get_data("/foo/bar")

        self.assertEqual(result, u"wooo")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.GetDataRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.watch, False)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_get_data_with_watch(self, Session):
        response = Mock(data=u"wooo")
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3")

        yield c.get_data("/foo/bar", watch=True)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.GetDataRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.watch, True)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_get_data_with_chroot(self, Session):
        response = Mock(data=u"wooo")
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3", chroot="bwee")

        yield c.get_data("/foo/bar", watch=True)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.GetDataRequest)
        self.assertEqual(request.path, "/bwee/foo/bar")
        self.assertEqual(request.watch, True)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_set_data(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")

        yield c.set_data("/foo/bar", data="some data")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.SetDataRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.data, "some data")
        self.assertEqual(request.version, -1)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_set_data_with_chroot(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3", chroot="/bar")

        yield c.set_data("/foo/bar", data=u"{json}")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.SetDataRequest)
        self.assertEqual(request.path, "/bar/foo/bar")
        self.assertEqual(request.data, u"{json}")
        self.assertEqual(request.version, -1)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_set_data_with_stat_cache_unforced(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")
        c.stat_cache["/foo/bar"] = Mock(version=33)

        yield c.set_data("/foo/bar", data="{json}")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.SetDataRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.data, "{json}")
        self.assertEqual(request.version, 33)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_set_data_with_stat_cache_forced(self, Session):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")
        c.stat_cache["/foo/bar"] = Mock(version=33)

        yield c.set_data("/foo/bar", data="{blarg}", force=True)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.SetDataRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.data, "{blarg}")
        self.assertEqual(request.version, -1)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_get_children(self, Session):
        response = Mock(children=["bwee", "bwoo"])
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3")

        result = yield c.get_children("/foo/bar")

        self.assertEqual(result, ["bwee", "bwoo"])

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.GetChildren2Request)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.watch, False)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_get_children_with_watch(self, Session):
        response = Mock(children=["bwee", "bwoo"])
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3")

        yield c.get_children("/foo/bar", watch=True)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.GetChildren2Request)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.watch, True)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_get_children_with_chroot(self, Session):
        response = Mock(children=["bwee", "bwoo"])
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3", chroot="bwee")

        yield c.get_children("/foo/bar", watch=True)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.GetChildren2Request)
        self.assertEqual(request.path, "/bwee/foo/bar")
        self.assertEqual(request.watch, True)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_get_acl(self, Session):
        response = Mock(children=["bwee", "bwoo"])
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3", chroot="bwee")

        yield c.get_acl("/foo/bar")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.GetACLRequest)
        self.assertEqual(request.path, "/bwee/foo/bar")

    @patch.object(client, "Session")
    @testing.gen_test
    def test_get_acl_with_chroot(self, Session):
        response = Mock(children=["bwee", "bwoo"])
        Session.return_value.send.return_value = self.future_value(response)

        c = client.Zoonado("host1,host2,host3", chroot="bwee")

        yield c.get_acl("/foo/bar")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.GetACLRequest)
        self.assertEqual(request.path, "/bwee/foo/bar")

    @patch.object(client, "Session")
    @testing.gen_test
    def test_set_acl(self, Session):
        Session.return_value.send.return_value = self.future_value(None)
        mock_acl = Mock()

        c = client.Zoonado("host1,host2,host3")

        yield c.set_acl("/foo/bar", acl=[mock_acl])

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.SetACLRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.acl, [mock_acl])
        self.assertEqual(request.version, -1)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_set_acl_with_chroot(self, Session):
        Session.return_value.send.return_value = self.future_value(None)
        mock_acl = Mock()

        c = client.Zoonado("host1,host2,host3", chroot="/bar")

        yield c.set_acl("/foo/bar", acl=[mock_acl])

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.SetACLRequest)
        self.assertEqual(request.path, "/bar/foo/bar")
        self.assertEqual(request.acl, [mock_acl])
        self.assertEqual(request.version, -1)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_set_acl_with_stat_cache_unforced(self, Session):
        Session.return_value.send.return_value = self.future_value(None)
        mock_acl = Mock()

        c = client.Zoonado("host1,host2,host3")
        c.stat_cache["/foo/bar"] = Mock(version=33)

        yield c.set_acl("/foo/bar", acl=[mock_acl])

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.SetACLRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.acl, [mock_acl])
        self.assertEqual(request.version, 33)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_set_acl_with_stat_cache_forced(self, Session):
        Session.return_value.send.return_value = self.future_value(None)
        mock_acl = Mock()

        c = client.Zoonado("host1,host2,host3")
        c.stat_cache["/foo/bar"] = Mock(version=33)

        yield c.set_acl("/foo/bar", acl=[mock_acl], force=True)

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertIsInstance(request, protocol.SetACLRequest)
        self.assertEqual(request.path, "/foo/bar")
        self.assertEqual(request.acl, [mock_acl])
        self.assertEqual(request.version, -1)

    @patch.object(client, "Transaction")
    def test_begin_transation_returns_transaction_object(self, Transaction):
        c = client.Zoonado("host1,host2,host3")

        txn = c.begin_transaction()

        self.assertEqual(txn, Transaction.return_value)
        Transaction.assert_called_once_with(c)

    @patch.object(client, "Features")
    @testing.gen_test
    def test_create_container_requires_feature_present(self, Features):
        Features.return_value.containers = False

        c = client.Zoonado("host1,host2,host3")

        with self.assertRaises(ValueError):
            yield c.create("/foo", container=True)

    @patch.object(client, "Features")
    @patch.object(client, "Session")
    @testing.gen_test
    def test_create_with_stat(self, Session, Features):
        response = Mock(path="/foo")

        Session.return_value.send.return_value = self.future_value(response)
        Features.return_value.create_with_stat = True

        c = client.Zoonado("host1,host2,host3")

        result = yield c.create("/foo", data="bar")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertEqual(result, "/foo")

        self.assertIsInstance(request, protocol.Create2Request)
        self.assertEqual(request.data, "bar")
        self.assertEqual(request.flags, 0)

    @patch.object(client, "Features")
    @patch.object(client, "Session")
    @testing.gen_test
    def test_create_without_stat(self, Session, Features):
        response = Mock(path="/foo")

        Session.return_value.send.return_value = self.future_value(response)
        Features.return_value.create_with_stat = False

        c = client.Zoonado("host1,host2,host3")

        result = yield c.create("/foo", data="bar")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertEqual(result, "/foo")

        self.assertIsInstance(request, protocol.CreateRequest)

    @patch.object(client, "Features")
    @patch.object(client, "Session")
    @testing.gen_test
    def test_create_with_chroot(self, Session, Features):
        response = Mock(path="/root/foo")

        Session.return_value.send.return_value = self.future_value(response)
        Features.return_value.create_with_stat = True

        c = client.Zoonado("host1,host2,host3", chroot="/root/")

        result = yield c.create("/foo", data="bar")

        args, kwargs = c.session.send.call_args
        request, = args

        self.assertEqual(result, "/foo")

    @patch.object(client, "Features")
    @patch.object(client, "Session")
    @testing.gen_test
    def test_ensure_path(self, Session, Features):
        Session.return_value.send.return_value = self.future_value(None)
        Features.return_value.create_with_stat = True

        c = client.Zoonado("host1,host2,host3")

        yield c.ensure_path("/foo/bar/bazz")

        requests = [arg[0] for arg, _ in c.session.send.call_args_list]

        self.assertIsInstance(requests[0], protocol.Create2Request)

        self.assertEqual(requests[0].path, "/foo")
        self.assertEqual(requests[1].path, "/foo/bar")
        self.assertEqual(requests[2].path, "/foo/bar/bazz")

    @patch.object(client, "Features")
    @patch.object(client, "Session")
    @testing.gen_test
    def test_ensure_path_is_normalized(self, Session, Features):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3", chroot="/blee")

        yield c.ensure_path("bar/bazz")

        requests = [arg[0] for arg, _ in c.session.send.call_args_list]

        self.assertEqual(requests[0].path, "/blee")
        self.assertEqual(requests[1].path, "/blee/bar")
        self.assertEqual(requests[2].path, "/blee/bar/bazz")

    @patch.object(client, "Features")
    @patch.object(client, "Session")
    @testing.gen_test
    def test_ensure_path_existing_nodes(self, Session, Features):
        send_results = [exc.NodeExists(), exc.NodeExists(), None]

        def get_send_result(*args):
            result = send_results.pop(0)
            if isinstance(result, Exception):
                return self.future_error(result)
            else:
                return self.future_value(result)

        Session.return_value.send.side_effect = get_send_result

        c = client.Zoonado("host1,host2,host3")

        yield c.ensure_path("/foo/bar/bazz")

        requests = [arg[0] for arg, _ in c.session.send.call_args_list]

        self.assertEqual(requests[0].path, "/foo")
        self.assertEqual(requests[1].path, "/foo/bar")
        self.assertEqual(requests[2].path, "/foo/bar/bazz")

    @patch.object(client, "Features")
    @patch.object(client, "Session")
    @testing.gen_test
    def test_ensure_path_without_stats(self, Session, Features):
        Session.return_value.send.return_value = self.future_value(None)

        c = client.Zoonado("host1,host2,host3")

        yield c.ensure_path("/foo/bar/bazz")

        requests = [arg[0] for arg, _ in c.session.send.call_args_list]

        self.assertIsInstance(requests[0], protocol.CreateRequest)
        self.assertIsInstance(requests[1], protocol.CreateRequest)
        self.assertIsInstance(requests[2], protocol.CreateRequest)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_wait_for_event_uses_session_callback_api(self, Session):
        session = Session.return_value

        c = client.Zoonado("host1,host2,host3")

        wait = c.wait_for_event(WatchEvent.CREATED, "/foo/bar")

        self.assertFalse(wait.done())

        args, _ = session.add_watch_callback.call_args
        added_event_type, added_path, added_callback = args

        self.assertEqual(added_event_type, WatchEvent.CREATED)
        self.assertEqual(added_path, "/foo/bar")

        self.assertFalse(session.remove_watch_callback.called)

        added_callback(wait)

        self.assertTrue(wait.done())
        self.assertEqual(wait.result(), None)

        args, _ = session.remove_watch_callback.call_args
        removed_event_type, removed_path, removed_callback = args

        self.assertEqual(added_event_type, removed_event_type)
        self.assertEqual(added_path, removed_path)
        self.assertEqual(added_callback, removed_callback)

    @patch.object(client, "Session")
    @testing.gen_test
    def test_wait_for_event_handles_multiple_calls(self, Session):
        session = Session.return_value

        c = client.Zoonado("host1,host2,host3")

        wait = c.wait_for_event(WatchEvent.CREATED, "/foo/bar")

        self.assertFalse(wait.done())

        args, _ = session.add_watch_callback.call_args
        added_event_type, added_path, added_callback = args

        self.assertEqual(added_event_type, WatchEvent.CREATED)
        self.assertEqual(added_path, "/foo/bar")

        self.assertFalse(session.remove_watch_callback.called)

        added_callback(wait)
        added_callback(wait)

        self.assertTrue(wait.done())
        self.assertEqual(wait.result(), None)
