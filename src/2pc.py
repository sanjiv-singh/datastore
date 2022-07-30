import random

# Transaction Manager types
COORDINATOR_TYPE = 'Coordinator'
PARTICIPANT_TYPE = 'Participant'

# States of both coordinator and participant during commit protocol
INITIAL_STATE = 'Initial State'
UNDECIDED_STATE = 'Undecided State'
READY_STATE = 'Ready State'
COMMIT_STATE = 'Commit State'
ABORT_STATE = 'Abort State'


class TxManager:
    """
    Transaction Manager type is provided at the creation time.
    The manager stores a list of transactions and sibling nodes
    which will participate in the distributed commit.
    It also stores a pointer to the coordinator of the group
    It has a transaction counter to provide each transaction a unique id
    """

    def __init__(self, tm_type):
        self._type = tm_type
        self._transactions = {}
        self._siblings = []
        if (tm_type == COORDINATOR_TYPE):
            self._coordinator = self
        else:
            self._coordinator = None
        self._transaction_counter = 0

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, tm_type):
        self._type = tm_type

    def add_sibling_nodes(self, sibling_list):
        "Adds sibling nodes for the transaction manager"
        self._siblings.extend(sibling_list)

    def make_coordinator(self):
        "Mark sekf as the coordinator in case a coordinator fails."
        self.type = COORDINATOR_TYPE
        self._coordinator = self

    def set_coordinator(self, coordinator):
        "Mark the new coordinator (in participant), when coordinator fails."
        self._coordinator = coordinator

    # This initiates a transaction in a coordinator or participant
    # A coordinator is not passed the id and will create a local transaction based on the latest counter
    # A participant is passed the id based on what the coordinator created
    # It'll create a local transaction with the same id counter
    def init_transaction(self, id=None):
        if (id is None):
            self._transaction_counter += 1
            id = self._transaction_counter
        else:
            self._transaction_counter = id
        transaction = Transaction(id, self)
        self._transactions[id] = transaction
        return transaction

    def get_stable_storage(self):
        "Returns a file handle as stable storage for writing commit log."
        stable_storage = StableStorage("/dev/null")
        return stable_storage



    def initiate_2pc(self, transaction_id):
        "Initiate two phase commit protocol (Phase 1)"
        if (self.type != COORDINATOR_TYPE):
            print("2pc initiation called on participant node. Bailing out...")
            return -1

        prepare_status = self.trigger_prepare_for_commit(transaction_id)

        #if (prepare_status):
            # EXERCISE FOR THE LEARNER
            # Write code for the second phase based on the algorithm detailed in the video content
            # Get context from how the first phase is implemented and follow the same logic
        if prepare_status:
            self.complete_2pc(transaction_id)     # Phase 2


    def complete_2pc(self, transaction_id):
        "Complete two phase commit protocol (Phase 2)"

        with self.get_stable_storage() as stable_storage:
            stable_storage.write("commit data")

        # Send COMMIT msg to all participants
        for participant in self._siblings:
            result = participant.do_commit(transaction_id)
            if result != 'Ack':
                print('Initiating Abort')
                transaction._state = ABORT_STATE
                for participant in self._siblings:
                    participant.abort_commit(transaction_id)
                return None
        print('Ack received from all participants')
        self.do_commit(transaction_id)


    def trigger_prepare_for_commit(self, transaction_id):
        "Start Phase 1 of 2PC from the coordinator."
        transaction = self._transactions[transaction_id]
        transaction.state = UNDECIDED_STATE
        prepare_status = True

        # The coordinator enters UNDECIDED_STATE and asks all participants
        # to prepare for commit
        for participant in self._siblings:
            result = participant.prepare_for_commit(transaction_id)

            # If any participant returns AAM, then the prepare is marked as failed
            if (result == 'AAM'):
                prepare_status = False

        # If prepare was rejected (or timed out) at even a single participant, abort message should be sent to all participants
        # The coordinator will mark its own local transaction as ABORT_STATE and ask all participants to do the same
        if (not prepare_status):
            transaction._state = ABORT_STATE
            for participant in self._siblings:
                participant.abort_commit(transaction_id)
        else:
            transaction._state = READY_STATE
        return prepare_status


    # This method will be called on a participant
    # We have made failure a possibility with 20% probability to naturally highlight all cases on reruns
    # Either all participants can succeed, OR one or more will failed
    # Even a single participant will lead to overall commit failure
    def prepare_for_commit(self, transaction_id):
        transaction = self._transactions[transaction_id]

        success_choice = random.choices(population=[True, False], weights=[0.8,0.2], k=1)[0]

        if (success_choice):
            print(f'Participant transaction responded with READY Message')
            transaction._state = READY_STATE
            return 'Ready'
        else:
            print(f'Participant transaction responded with AAM Message')
            transaction._state = ABORT_STATE
            return 'AAM'

    def abort_commit(self, transaction_id):
        transaction = self._transactions[transaction_id]
        transaction.state = ABORT_STATE

    def do_commit(self, transaction_id):
        transaction = self._transactions[transaction_id]
        assert transaction.state == READY_STATE
        print(f'Commiting data')
        transaction.state = COMMIT_STATE
        return'Ack'


class Transaction:
    def __init__(self, id, tx_manager):
        self._id = id
        self._tx_manager = tx_manager
        self._state = INITIAL_STATE

    def __str__(self):
        return f'Transaction id = {self._id}, with state \'{self._state}\''

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

class StableStorage:

    def __init__(self, file_path):
        "Get a stable storage file handle from os."
        self.file_path = file_path

    def __enter__(self):
        print("Acquiring stable storage for commit log")
        self.file_obj = open(self.file_path, mode="w")
        return self.file_obj

    def __exit__(self, exc_type, exc_value, exc_tb):
        print("Releasing stable storage for commit log")
        if self.file_obj:
            self.file_obj.close()


# Initiate transaction manager on three nodes
txm_coord = TxManager(COORDINATOR_TYPE)
txm_part1 = TxManager(PARTICIPANT_TYPE)
txm_part2 = TxManager(PARTICIPANT_TYPE)

# Add sibling nodes in each manager
txm_coord.add_sibling_nodes([txm_part1, txm_part2])
txm_part1.add_sibling_nodes([txm_coord, txm_part2])
txm_part2.add_sibling_nodes([txm_coord, txm_part1])

txm_part1.set_coordinator(txm_coord)
txm_part2.set_coordinator(txm_coord)


# Initialize transactions
coord_tx = txm_coord.init_transaction()
tx_id = coord_tx.id
part1_tx = txm_part1.init_transaction(tx_id)
part2_tx = txm_part2.init_transaction(tx_id)

print('Initial Transaction State:')
print(f'Coordinator Tx: {coord_tx}')
print(f'Participant 1 Tx: {part1_tx}')
print(f'Participant 2 Tx: {part2_tx}')
print('\n')


txm_coord.initiate_2pc(tx_id)


print('\nTransaction State After First Phase:')
print(f'Coordinator Tx: {coord_tx}')
print(f'Participant 1 Tx: {part1_tx}')
print(f'Participant 2 Tx: {part2_tx}')
