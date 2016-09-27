from tornado import testing

from zoonado import states


class StatesTests(testing.AsyncTestCase):

    def setUp(self):
        super(StatesTests, self).setUp()

        self.fsm = states.SessionStateMachine()

    def test_defaults_to_lost_state(self):
        self.assertEqual(self.fsm.current_state, states.States.LOST)

    def test_fsm_equality(self):
        assert self.fsm == states.States.LOST
        assert self.fsm != states.States.CONNECTED
        assert self.fsm != states.States.SUSPENDED

        self.fsm.transition_to(states.States.CONNECTED)

        assert self.fsm != states.States.LOST
        assert self.fsm == states.States.CONNECTED
        assert self.fsm != states.States.SUSPENDED

    @testing.gen_test
    def test_waiting_for_a_state(self):
        wait = self.fsm.wait_for(states.States.CONNECTED)

        assert wait.done() is False

        self.fsm.transition_to(states.States.CONNECTED)

        yield wait

    @testing.gen_test
    def test_waiting_for_any_of_a_few_states(self):
        wait = self.fsm.wait_for(
            states.States.CONNECTED, states.States.READ_ONLY
        )

        assert wait.done() is False

        self.fsm.transition_to(states.States.CONNECTED)

        yield wait

        self.fsm.transition_to(states.States.LOST)

        wait = self.fsm.wait_for(
            states.States.CONNECTED, states.States.READ_ONLY
        )

        assert wait.done() is False

        self.fsm.transition_to(states.States.READ_ONLY)

        yield wait

    @testing.gen_test
    def test_waiting_on_current_state_yield_immediately(self):
        yield self.fsm.wait_for(states.States.LOST)

    def test_valid_transitions(self):
        # lost/initial sessions can connect
        assert self.fsm == states.States.LOST
        self.fsm.transition_to(states.States.CONNECTED)

        # connected sessions can be suspended
        assert self.fsm == states.States.CONNECTED
        self.fsm.transition_to(states.States.SUSPENDED)

        # suspended session can reconnect
        assert self.fsm == states.States.SUSPENDED
        self.fsm.transition_to(states.States.CONNECTED)

        # connected sessions can be lost (when closing)
        assert self.fsm == states.States.CONNECTED
        self.fsm.transition_to(states.States.LOST)

        # lost sessions can reconnect as read-only
        assert self.fsm == states.States.LOST
        self.fsm.transition_to(states.States.READ_ONLY)

        # read-only sessions can become fully connected
        assert self.fsm == states.States.READ_ONLY
        self.fsm.transition_to(states.States.CONNECTED)

        self.fsm.transition_to(states.States.SUSPENDED)

        # suspended sessions can reconnect as read-only
        assert self.fsm == states.States.SUSPENDED
        self.fsm.transition_to(states.States.READ_ONLY)

        # read-only sessions can be lost (when closing)
        assert self.fsm == states.States.READ_ONLY
        self.fsm.transition_to(states.States.LOST)

        self.fsm.transition_to(states.States.READ_ONLY)

        # read-only sessions can be suspended
        assert self.fsm == states.States.READ_ONLY
        self.fsm.transition_to(states.States.SUSPENDED)

        # suspended connections can be lost
        assert self.fsm == states.States.SUSPENDED
        self.fsm.transition_to(states.States.LOST)

    def test_invalid_transitions(self):
        # lost sessions cannot be re-lost
        with self.assertRaises(RuntimeError):
            self.fsm.transition_to(states.States.LOST)

        # lost sessions cannot be suspended
        with self.assertRaises(RuntimeError):
            self.fsm.transition_to(states.States.SUSPENDED)

        self.fsm.transition_to(states.States.CONNECTED)

        # connected sessions cannot become read-only (suspended or lost first)
        with self.assertRaises(RuntimeError):
            self.fsm.transition_to(states.States.READ_ONLY)

        # connected sessions cannot be connected again
        with self.assertRaises(RuntimeError):
            self.fsm.transition_to(states.States.CONNECTED)

        self.fsm.transition_to(states.States.SUSPENDED)

        # suspended sessions cannot be re-suspended
        with self.assertRaises(RuntimeError):
            self.fsm.transition_to(states.States.SUSPENDED)

        self.fsm.transition_to(states.States.READ_ONLY)

        # read-only sessions cannot be made read-only twice
        with self.assertRaises(RuntimeError):
            self.fsm.transition_to(states.States.READ_ONLY)
