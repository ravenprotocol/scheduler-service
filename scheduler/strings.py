class OpTypes(object):
    UNARY = "unary"
    BINARY = "binary"
    OTHER = "other"


class NodeTypes(object):
    INPUT = "input"
    MIDDLE = "middle"
    OUTPUT = "output"


class TFJSOperators(object):
    SIGMOID = "sigmoid"
    SIN = "sin"
    SINH = "sinh"
    SOFTPLUS = "softplus"


class Status(object):
    PENDING = "pending"
    COMPUTED = "computed"
    FAILED = "failed"
    COMPUTING = "computing"


class OpStatus(Status):
    READY = "ready"


class GraphStatus(Status):
    READY = "ready"
    IDLE = "idle"


class SubgraphStatus(Status):
    READY = "ready"


class MappingStatus(Status):
    SENT = "sent"
    ACKNOWLEDGED = "acknowledged"
    NOT_ACKNOWLEDGED = "not_acknowledged"
    NOT_COMPUTED = "not_computed"
    REJECTED = "rejected"


class ClientStatus(object):
    CONNECTED = "connected"
    COMPUTING = "computing"
    IDLE = "idle"
    DISCONNECTED = "disconnected"
    READY = "ready"


class OpReadiness(object):
    READY = "ready"
    NOT_READY = "not_ready"
    PARENT_OP_FAILED = "parent_op_failed"
    PARENT_OP_NOT_READY = "parent_op_not_ready"