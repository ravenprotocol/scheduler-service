import datetime

from sqlalchemy import Column, Float, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Graph(Base):
    __tablename__ = "graph"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=True, default=None)

    compiled = Column(String(10), nullable=True, default="False")

    execute = Column(String(10), nullable=True, default="False")

    algorithm = Column(String(50), nullable=True, default=None)  # mean, mode, linear_regression, logistic
    approach = Column(String(50), nullable=True, default=None)  # distributed, federated

    #Active Participants
    active_participants = Column(Integer, nullable=False, default=0)

    #Required Participants
    required_participants = Column(Integer, nullable=False, default=1)

    #Proportioned
    proportioned = Column(Text, nullable=True, default="False")

    started = Column(Text, nullable=True, default="False")
    
    cost = Column(Float, nullable=True, default=0)

    #Threshold system requirements
    system_requirements = Column(Text, nullable=True, default=None)

    # Subgrapgh Complexities
    subgraph = Column(Text, nullable=True)

    # Rules
    rules = Column(Text, nullable=True, default=None)

    ops = relationship("Op", backref="graph")

    # Status of this graph 1. pending 2. computing 3. computed 4. failed
    status = Column(String(10), default="pending")

    failed_subgraph = Column(Text, nullable=True, default="False")

    message = Column(Text, nullable=True)

    owner = Column(String(100), nullable=True)

    min_split_size = Column(Integer, default=100)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class Data(Base):
    __tablename__ = "data"
    id = Column(Integer, primary_key=True)
    dtype = Column(String(20), nullable=False)
    file_path = Column(String(200), nullable=True)
    file_size = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class Client(Base):
    __tablename__ = "client"
    id = Column(Integer, primary_key=True)
    cid = Column(String(100), nullable=False)
    token = Column(Text, nullable=False)
    role = Column(String(20), nullable=True)
    sid = Column(String(100), nullable=False)
    affiliated_graph_id = Column(Integer, nullable=True, default=None)

    #Stake and proportion
    stake = Column(Float, nullable=True, default=0)
    proportion = Column(Integer, nullable=True)

    original_proportion = Column(Integer, nullable=True)

    staked_amount = Column(Float(precision=32), nullable=True, default=0)
    stashed_queue = Column(Text, nullable=True, default=None)

    status = Column(String(20), nullable=False, default="disconnected")
    port = Column(Integer, nullable=True, default=None)

    client_sids = relationship("ClientSIDMapping", backref="client", lazy="dynamic")

    # 1. ravop 2. ravjs
    type = Column(String(10), nullable=True)
    client_ops = relationship("ClientOpMapping", backref="client", lazy="dynamic")

    reporting = Column(String(20), nullable=False, default="ready")
    ftp_credentials = Column(String(100), nullable=True, default=None)
    context = Column(Text, nullable=True, default=None)

    current_subgraph_id = Column(Integer, nullable=True, default=None)
    current_graph_id = Column(Integer, nullable=True, default=None)

    connected_at = Column(DateTime, default=datetime.datetime.utcnow)
    disconnected_at = Column(DateTime, default=datetime.datetime.utcnow)

    last_active_time = Column(DateTime, default=datetime.datetime.utcnow)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class ClientSIDMapping(Base):
    __tablename__ = "client_sid_mapping"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("client.id"))
    cid = Column(String(100), nullable=False)
    sid = Column(String(100), nullable=False)
    namespace = Column(String(100), nullable=False)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class SubGraph(Base):
    __tablename__ = "sub_graph"
    id = Column(Integer, primary_key=True)

    graph_id = Column(Integer, ForeignKey("graph.id"))

    subgraph_id = Column(Integer, nullable=True, default=1)

    assigned_client = Column(String(100), nullable=True)

    cost = Column(Float, nullable=True, default=0)

    optimized = Column(String(10), nullable=True, default="False")

    parent_subgraph_id = Column(Integer, nullable=True, default=None)

    retry_attempts = Column(Integer, nullable=True, default=0)

    has_failed = Column(String(10), nullable=True, default="False")

    # ops = relationship("Op", backref="sub_graph")
    op_ids = Column(Text, nullable=True)

    complexity = Column(Integer, nullable=True, default=0)

    # 1. pending 2. computing 3. computed 4. failed
    status = Column(String(10), default="pending")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class Op(Base):
    __tablename__ = "op"
    id = Column(Integer, primary_key=True)

    # Op name
    name = Column(Text, nullable=True)

    # Persists or not
    persist = Column(String(10), nullable=True, default="False")

    # Graph id
    graph_id = Column(Integer, ForeignKey("graph.id"))

    # Subgraph id
    subgraph_id = Column(Integer, nullable=True)
    complexity = Column(Float, nullable=True)

    billed = Column(String(10), nullable=True, default="False")
    # output_dims = Column(Text, nullable=True, default=None)

    # Store list of op ids
    inputs = Column(Text, nullable=True)

    # Store list of data ids
    outputs = Column(Text, nullable=True)

    dependents = Column(Text, nullable=True)

    # 1. input 2. output 3. middle
    node_type = Column(String(10), nullable=True)
    op_type = Column(String(50), nullable=True)
    operator = Column(String(50), nullable=True)
    message = Column(Text, nullable=True)

    # 1. pending 2. computing 3. computed 4. failed
    status = Column(String(10), default="pending")

    # Dict of params
    params = Column(Text, nullable=True)

    op_mappings = relationship("ClientOpMapping", backref="op", lazy="dynamic")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class ClientOpMapping(Base):
    __tablename__ = "client_op_mapping"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("client.id"))
    op_id = Column(Integer, ForeignKey("op.id"))
    sent_time = Column(DateTime, default=None)
    response_time = Column(DateTime, default=None)

    # 1. computing 2. computed 3. failed
    status = Column(String(10), default="computing")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class GraphClientMapping(Base):
    __tablename__ = "graph_client_mapping"
    id = Column(Integer, primary_key=True)
    graph_id = Column(Integer, ForeignKey("graph.id"))
    client_id = Column(Integer, ForeignKey("client.id"))
    sent_time = Column(DateTime, default=None)
    response_time = Column(DateTime, default=None)

    # 1. computing 2. computed 3. failed
    status = Column(String(10), default="computing")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class SubgraphClientMapping(Base):
    __tablename__ = "subgraph_client_mapping"
    id = Column(Integer, primary_key=True)
    graph_id = Column(Integer, ForeignKey('graph.id'))
    subgraph_id = Column(Integer, ForeignKey("sub_graph.id"))
    client_id = Column(Integer, ForeignKey("client.id"))
    sent_time = Column(DateTime, default=None)
    response_time = Column(DateTime, default=None)
    result = Column(Text, nullable=True)

    # 1. computing 2. computed 3. failed
    status = Column(String(10), default="pending")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


"""
Federated and analytics
"""


class Objective(Base):
    __tablename__ = "objective"
    id = Column(Integer, primary_key=True)
    graph_id = Column(Integer, ForeignKey("graph.id"))
    name = Column(String(50), nullable=True, default=None)
    operator = Column(String(50), nullable=True, default=None)
    rules = Column(Text, nullable=True, default=None)

    # Store a list of data id
    inputs = Column(Text, nullable=True)

    result = Column(Text, nullable=True, default=None)
    # 1. pending 2. active 3. completed 4. failed
    status = Column(String(10), default="pending")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class ObjectiveClientMapping(Base):
    __tablename__ = "objective_client_mapping"
    id = Column(Integer, primary_key=True)
    objective_id = Column(Integer, ForeignKey("objective.id"))
    client_id = Column(Integer, ForeignKey("client.id"))
    sent_time = Column(DateTime, default=None)
    response_time = Column(DateTime, default=None)
    input_id = Column(Integer, ForeignKey("op.id"))
    result = Column(Text, default=None)

    # 1. computing 2. computed 3. failed
    status = Column(String(10), default="computing")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)
