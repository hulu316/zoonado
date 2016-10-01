from mock import patch, Mock, call
from tornado import testing, concurrent

from zoonado.protocol.connect import ConnectRequest, ConnectResponse
from zoonado import connection


class ConnectionTests(testing.AsyncTestCase):

    def setUp(self):
        super(ConnectionTests, self).setUp()

        self.response_buffer = bytearray()

        tcpclient_patcher = patch.object(connection, "tcpclient")
        mock_tcpclient = tcpclient_patcher.start()
        self.addCleanup(tcpclient_patcher.stop)

        self.mock_client = mock_tcpclient.TCPClient.return_value

        stream = Mock()
        self.mock_client.connect.return_value = self.future_value(stream)

        def read_some(num_bytes):
            result = self.response_buffer[:num_bytes]
            del self.response_buffer[:num_bytes]

            return self.future_value(result)

        def read_all():
            result = self.response_buffer[:]

            self.response_buffer = bytearray()

            return self.future_value(result)

        stream.write.return_value = self.future_value(None)
        stream.read_bytes.side_effect = read_some
        stream.read_until_close.side_effect = read_all

    def future_value(self, value):
        f = concurrent.Future()
        f.set_result(value)
        return f

    def future_error(self, exception):
        f = concurrent.Future()
        f.set_exception(exception)
        return f

    @testing.gen_test
    def test_connect_gets_version_info(self):
        self.response_buffer.extend(
            b"""Zookeeper version: 3.4.6-1569965, built on 02/20/2014 09:09 GMT
Latency min/avg/max: 0/0/1137
Received: 21462
Sent: 21474
Connections: 2
Outstanding: 0
Zxid: 0x11171
Mode: standalone
Node count: 232"""
        )

        conn = connection.Connection("local", 9999, Mock())

        yield conn.connect()

        self.assertEqual(conn.version_info, (3, 4, 6))
        self.assertEqual(conn.start_read_only, False)

        self.mock_client.connect.assert_has_calls([
            call("local", 9999),
            call("local", 9999),
        ])

    @testing.gen_test
    def test_send_connect_returns_none_on_error(self):
        conn = connection.Connection("local", 9999, Mock())

        conn.stream = Mock()
        conn.stream.write.return_value = self.future_value(None)
        conn.stream.read_bytes.return_value = self.future_error(Exception("!"))

        result = yield conn.send_connect(
            ConnectRequest(
                protocol_version=0,
                last_seen_zxid=0,
                timeout=8000,
                session_id=0,
                password=b'\x00',
                read_only=False,
            )
        )

        self.assertEqual(result, None)

    @patch.object(connection.Connection, "read_response")
    @testing.gen_test
    def test_send_connect(self, read_response):
        conn = connection.Connection("local", 9999, Mock())
        conn.stream = Mock()
        conn.stream.write.return_value = self.future_value(None)

        response = ConnectResponse(
            protocol_version=0,
            timeout=7000,
            session_id=123456,
            password=b"\xc3"
        )

        read_response.return_value = self.future_value(
            (
                23,  # xid
                123,  # zxid
                response,  # response
            )
        )

        result = yield conn.send_connect(
            ConnectRequest(
                protocol_version=0,
                last_seen_zxid=0,
                timeout=8000,
                session_id=0,
                password=b'\x00',
                read_only=False,
            )
        )

        self.assertEqual(result, (123, response))
