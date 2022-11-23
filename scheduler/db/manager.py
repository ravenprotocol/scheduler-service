from __future__ import annotations

import ast
import json
import numpy as np
import sqlalchemy as db
from sqlalchemy import and_
from sqlalchemy import or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database as cd
from sqlalchemy_utils import database_exists
from sqlalchemy_utils import drop_database as dba

from .models import Base
from .models import Client
from .models import ClientOpMapping
from .models import ClientSIDMapping
from .models import Data
from .models import Graph
from .models import GraphClientMapping
from .models import Objective
from .models import ObjectiveClientMapping
from .models import Op
from .models import SubGraph
from .models import SubgraphClientMapping
from ..config import MINIMUM_SPLIT_SIZE
from ..config import RAVENVERSE_DATABASE_URI
from ..strings import MappingStatus, OpStatus
from ..utils import delete_data_file
from ..utils import load_data_from_file
from ..utils import save_data_to_file


class DBManager:
    def __init__(self):
        self.create_database()
        self.engine = self.connect()
        self.logger = None

    def set_logger(self, logger):
        self.logger = logger

    def connect(self):
        # print("Database uri:", RAVENVERSE_DATABASE_URI)
        engine = db.create_engine(RAVENVERSE_DATABASE_URI, isolation_level='READ UNCOMMITTED')
        Base.metadata.bind = engine
        return engine

    def create_database(self):
        if not database_exists(RAVENVERSE_DATABASE_URI):
            cd(RAVENVERSE_DATABASE_URI)
            print('Database created')

    def drop_database(self):
        if database_exists(RAVENVERSE_DATABASE_URI):
            dba(RAVENVERSE_DATABASE_URI)
            print('Database dropped')

    def get_session(self):
        Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        return Session

    def create_session(self):
        """
        Create a new session
        """
        DBSession = sessionmaker(bind=self.engine)
        return DBSession()

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def refresh(self, obj):
        """
        Refresh an object
        """
        Session = self.get_session()
        with Session.begin() as session:
            session.refresh(obj)
            return obj

    def get(self, name, id):
        Session = self.get_session()
        with Session.begin() as session:
            if name == 'op':
                obj = session.query(Op).get(id)
            elif name == 'data':
                obj = session.query(Data).get(id)
            elif name == 'graph':
                obj = session.query(Graph).get(id)
            elif name == 'client':
                obj = session.query(Client).get(id)
            elif name == 'objective':
                obj = session.query(Objective).get(id)
            elif name == 'objective_client_mapping':
                obj = session.query(ObjectiveClientMapping).get(id)
            elif name == 'client_sid_mapping':
                obj = session.query(ClientSIDMapping).get(id)
            else:
                obj = None

            return obj

    def add(self, n, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            if n == 'op':
                obj = Op()
            elif n == 'data':
                obj = Data()
            elif n == 'graph':
                obj = Graph()
            elif n == 'client':
                obj = Client()
            elif n == 'objective':
                obj = Objective()
            elif n == 'objective_client_mapping':
                obj = ObjectiveClientMapping()
            elif n == 'client_sid_mapping':
                obj = ClientSIDMapping()
            else:
                obj = None

            for key, value in kwargs.items():
                setattr(obj, key, value)
            session.add(obj)

            return obj

    def update(self, n, id, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            if n == 'op':
                obj = session.query(Op).get(id)
            elif n == 'data':
                obj = session.query(Data).get(id)
            elif n == 'graph':
                obj = session.query(Graph).get(id)
            elif n == 'client':
                obj = session.query(Client).get(id)
            elif n == 'objective':
                obj = session.query(Objective).get(id)
            elif n == 'objective_client_mapping':
                obj = session.query(ObjectiveClientMapping).get(id)
            elif n == 'client_sid_mapping':
                obj = session.query(ClientSIDMapping).get(id)
            else:
                obj = None

            for key, value in kwargs.items():
                setattr(obj, key, value)
            session.add(obj)
            return obj

    def delete(self, obj):
        Session = self.get_session()
        with Session.begin() as session:
            session.delete(obj)

    def create_op(self, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            op = Op()

            for key, value in kwargs.items():
                setattr(op, key, value)

            session.add(op)
            return op

    def get_op(self, op_id):
        """
        Get an existing op
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Op).get(op_id)

    def get_ops_by_subgraph(self, subgraph_id, graph_id):
        """
        Get all ops of a subgraph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Op).filter(and_(Op.subgraph_id == subgraph_id, Op.graph_id == graph_id)).all()

    def delete_op(self, op):
        Session = self.get_session()
        with Session.begin() as session:
            session.delete(op)

    def update_op(self, op, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            for key, value in kwargs.items():
                setattr(op, key, value)
            session.add(op)
            return op

    def create_data(self, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            data = Data()

            for key, value in kwargs.items():
                setattr(data, key, value)

            session.add(data)
            return data

    def get_data(self, data_id):
        """
        Get an existing data
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Data).get(data_id)

    def update_data(self, data, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            for key, value in kwargs.items():
                setattr(data, key, value)
            session.add(data)
            return data

    def delete_data(self, data_id):
        Session = self.get_session()
        with Session.begin() as session:
            data = session.query(Data).get(data_id)
            session.delete(data)

    def create_data_complete(self, data, data_type):
        if isinstance(data, (np.ndarray, np.generic)):
            if data.ndim == 1:
                data = data[..., np.newaxis]

        d = self.create_data(type=data_type)

        # Save file
        file_path = save_data_to_file(d.id, data, data_type)

        # Update file path
        self.update(d, file_path=file_path)
        return d

    def get_op_status(self, op_id):
        Session = self.get_session()
        with Session.begin() as session:
            status = session.query(Op).get(op_id).status
            return status

    def get_last_graph_id(self):
        """
        Get the last graph id
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Graph).count() + 1

    def get_graph_op_dependency(self, graph_id, minimum_split_size=None):
        """
        Get op_dependencies of a given graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            if minimum_split_size is None:
                minimum_split_size = MINIMUM_SPLIT_SIZE
            max_subgraph_id = len(self.get_all_subgraphs(graph_id=graph_id))

            # ops = session.query(Op).filter(and_(Op.graph_id == graph_id, Op.status == 'pending')).limit(70).offset(140).all()
            start_index = session.query(Op).filter(and_(Op.graph_id == graph_id, Op.status == 'pending')).first()

            if start_index is not None:
                start_index = start_index.id

                ops = session.query(Op).filter(
                    and_(Op.id >= start_index, Op.id <= start_index + minimum_split_size * 2, Op.graph_id == graph_id,
                         Op.status == 'pending')).all()

                subgraph_ops = {}

                for op in ops:
                    if op.subgraph_id == 0:
                        if subgraph_ops.get(max_subgraph_id + 1, None) is None:
                            subgraph_ops[max_subgraph_id + 1] = [op.id]
                        else:
                            subgraph_ops[max_subgraph_id + 1].append(op.id)

                    else:
                        if subgraph_ops.get(op.subgraph_id, None) is None:
                            subgraph_ops[op.subgraph_id] = [op.id]
                        else:
                            subgraph_ops[op.subgraph_id].append(op.id)

                return subgraph_ops

            else:
                return {}

    def get_persisting_ops(self, graph_id):
        """
        Get all persisting ops from graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Op).filter(and_(Op.graph_id == graph_id, Op.persist == 'True')).all()

    def get_unemitted_ops(self, graph_id):
        """
        Get all unemitted ops from graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Op).filter(
                and_(Op.graph_id == graph_id, Op.subgraph_id == 0, Op.dependents is None),
            ).all()

    def get_graph(self, graph_id):
        """
        Get an existing graph
        """
        if graph_id is not None:
            Session = self.get_session()
            with Session.begin() as session:
                return session.query(Graph).get(graph_id)
        else:
            return None

    def create_graph(self):
        """
        Create a new graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            graph = Graph()
            session.add(graph)
            return graph

    def update_graph(self, graph, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            for key, value in kwargs.items():
                setattr(graph, key, value)
            session.add(graph)
            return graph

    # def update_graph(self, graph, **kwargs):
    #     for key, value in kwargs.items():
    #         setattr(graph, key, value)
    #     session.commit()
    #     return graph

    def get_non_dependency_ops(self, graph_id):
        """
        Get non dependency ops of a given graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Op).filter(
                and_(Op.graph_id == graph_id, Op.status == 'computed', Op.dependents == None),
            ).all()

    def get_graph_ops(self, graph_id):
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Op).filter(Op.graph_id == graph_id).all()

    def get_op_by_name(self, op_name, graph_id):
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Op).filter(and_(Op.graph_id == graph_id, Op.name == op_name)).first()

    def delete_graph_ops(self, graph_id):
        self.logger.debug('Deleting graph ops...')
        ops = self.get_graph_ops(graph_id=graph_id)

        for op in ops:
            # print("Op id:{}".format(op.id))
            data_ids = json.loads(op.outputs)
            if data_ids is not None:
                for data_id in data_ids:
                    # print("Data id:{}".format(data_id))

                    # print("Deleting Data file...")
                    # Delete data file
                    delete_data_file(data_id)

                    # print("Deleting Data object...")
                    # Delete data object
                    self.delete_data(data_id)

            # print("Deleting op...")
            # Delete op object
            self.delete(op)

    def create_client(self, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            obj = Client()

            for key, value in kwargs.items():
                setattr(obj, key, value)

            session.add(obj)
            return obj

    def get_client(self, id):
        """
        Get an existing client by id
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Client).get(id)

    def get_developer_from_graph(self, graph_id):
        """
        Get a the developer of a given graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Client).filter(
                and_(Client.current_graph_id == graph_id, Client.role == 'developer'),
            ).first()

    def get_client_from_token(self, token):
        """
        Get client by token
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Client).filter(Client.token == token).first()

    def get_assigned_client(self, subgraph_id, graph_id):
        """
        Get assigned client by subgraph_id and graph_id
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Client).filter(Client.status == 'connected').filter(
                Client.reporting != 'broken_connection',
            ).filter(Client.current_graph_id == graph_id).filter(
                Client.current_subgraph_id == subgraph_id,
            ).first()

    def get_assigned_clients(self, subgraph_id=None, graph_id=None, role=None):
        """
        Get all assigned client by subgraph_id and graph_id
        """
        if role is None:
            role = 'contributor'
        Session = self.get_session()
        with Session.begin() as session:
            if subgraph_id is not None:
                return session.query(Client).filter(Client.status == 'connected').filter(
                    Client.reporting != 'broken_connection',
                ).filter(Client.role == role).filter(
                    Client.current_graph_id == graph_id,
                ).filter(Client.current_subgraph_id == subgraph_id).all()
            else:
                return session.query(Client).filter(Client.status == 'connected').filter(
                    Client.reporting != 'broken_connection',
                ).filter(Client.role == role).filter(
                    Client.current_graph_id == graph_id,
                ).all()

    def get_assigned_clients_to_be_cleared(self, graph_id=None, role=None):
        """
        Get all assigned client by subgraph_id and graph_id
        """
        if role is None:
            role = 'contributor'
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Client).filter(Client.status == 'connected').filter(
                Client.reporting != 'broken_connection',
            ).filter(Client.role == role).filter(
                Client.current_graph_id == graph_id,
            ).all()

    def get_client_by_cid(self, cid):
        """
        Get an existing client by cid
        """
        Session = self.get_session()
        with Session.begin() as session:
            clients = session.query(Client).filter(Client.cid == cid)
            if clients.count() == 0:
                return None
            return clients.first()

    def update_client(self, client, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            for key, value in kwargs.items():
                setattr(client, key, value)
            session.add(client)
            return client

    def get_all_clients(self):
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Client).order_by(Client.created_at.desc()).all()

    def get_all_graphs(self, approach=None):
        if approach:
            Session = self.get_session()
            with Session.begin() as session:
                return session.query(Graph).filter(
                    and_(
                        Graph.status == 'pending', Graph.approach == approach, Graph.execute == 'True',
                        Graph.compiled == 'True',
                    ),
                ).order_by(Graph.created_at.desc()).all()

    def get_all_ops(self):
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Op).order_by(Op.id.desc()).all()

    # def deactivate_all_graphs(self):
    #     for graph in session.query(Graph).all():
    #         graph.status = "inactive"
    #
    #     session.commit()
    #
    # def deactivate_graph(self, graph_id):
    #     graph = self.get_graph(graph_id=graph_id)
    #     graph.status = "inactive"
    #     session.commit()

    def disconnect_all_clients(self):
        Session = self.get_session()
        with Session.begin() as session:
            for client in session.query(Client).all():
                client.status = 'disconnected'
                session.add(client)

    def disconnect_client(self, client_id):
        Session = self.get_session()
        with Session.begin() as session:
            client = self.get_client(client_id=client_id)
            client.status = 'disconnected'
            session.add(client)

    def get_ops_by_name(self, op_name, graph_id=None):
        Session = self.get_session()
        with Session.begin() as session:
            if graph_id is not None:
                return (session.query(Op).filter(Op.graph_id == graph_id).filter(Op.name.contains(op_name)).all())
            else:
                return session.query(Op).filter(Op.name.contains(op_name)).all()

    def get_op_readiness(self, op):
        """
        Get op readiness
        """
        inputs = json.loads(op.inputs)
        params = json.loads(op.params)

        cs = 0

        if inputs is not None:

            for input_op in inputs:
                input_op1 = self.get_op(op_id=input_op)
                if input_op1.status in ['pending', 'computing']:
                    return 'parent_op_not_ready'
                elif input_op1.status == 'failed':
                    return 'parent_op_failed'
                elif input_op1.status == 'computed':
                    cs += 1

            for index, value in params.items():
                if type(value).__name__ == 'int':
                    cop = self.get_op(op_id=value)
                    if cop.status in ['pending', 'computing']:
                        return 'parent_op_not_ready'
                    elif cop.status == 'failed':
                        return 'parent_op_failed'
                    elif cop.status == 'computed':
                        cs += 1
                else:
                    cs += 1

            if cs == len(inputs) + len(params.keys()):
                return 'ready'
            else:
                return 'not_ready'

        return 'ready'

    def get_ops_without_graph(self, status=None):
        """
        Get a list of all ops not associated to any graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            if status is not None:
                return (
                    session.query(Op)
                        .filter(Op.graph_id is None)
                        .filter(Op.status == status)
                        .all()
                )
            else:
                return session.query(Op).filter(Op.graph_id is None).all()

    def get_graphs(self, status=None, approach=None, execute=None):
        """
        Get a list of graphs
        """
        Session = self.get_session()
        with Session.begin() as session:
            graphs = session.query(Graph)
            if status is not None:
                graphs = graphs.filter(Graph.status == status)

            if approach is not None:
                graphs = graphs.filter(Graph.approach == approach)

            if execute is not None:
                graphs = graphs.filter(Graph.execute == execute)

            return graphs.all()

    def get_clients(self, status=None):
        """
        Get a list of clients
        """
        Session = self.get_session()
        with Session.begin() as session:
            if status is not None:
                return session.query(Client).filter(Client.role == 'contributor').filter(Client.status == status).all()
            else:
                return session.query(Client).filter(Client.role == 'contributor').all()

    def get_idle_connected_clients(self, status=None):
        """
        Get a list of clients
        """
        Session = self.get_session()
        with Session.begin() as session:
            if status is not None:
                return session.query(Client).filter(Client.role == 'contributor').filter(
                    or_(Client.reporting == 'idle', Client.reporting == 'busy'),
                ).filter(Client.status == status).all()
            else:
                return session.query(Client).filter(Client.role == 'contributor').filter(
                    or_(Client.reporting == 'idle', Client.reporting == 'busy'),
                ).all()

    def get_idle_clients(self, reporting=None):
        """
        Get a list of idle clients based on reporting column
        """
        Session = self.get_session()
        with Session.begin() as session:
            if reporting is not None:
                return session.query(Client).filter(Client.role == 'contributor').filter(
                    Client.status == 'connected',
                ).filter(Client.reporting == reporting).all()
            else:
                return session.query(Client).filter(Client.role == 'contributor').filter(
                    Client.status == 'connected',
                ).all()

    def get_available_clients(self):
        """
        Get all clients which are available
        """
        Session = self.get_session()
        with Session.begin() as session:
            clients = session.query(Client).filter(Client.role == 'contributor').filter(
                Client.status == 'connected',
            ).all()

            client_list = []
            for client in clients:
                client_ops = client.client_ops.filter(
                    or_(
                        ClientOpMapping.status == MappingStatus.SENT,
                        ClientOpMapping.status == MappingStatus.ACKNOWLEDGED,
                        ClientOpMapping.status == MappingStatus.COMPUTING,
                    ),
                )
                if client_ops.count() == 0:
                    client_list.append(client)

            return client_list

    def get_subgraph_ops(self, graph_id=None, subgraph_id=None):
        """
        Get a list of ops based on graph and subgraph ids
        """
        Session = self.get_session()
        with Session.begin() as session:
            if graph_id is None and subgraph_id is None:
                return session.query(Op).all()
            elif graph_id is not None and subgraph_id is not None:
                return (
                    session.query(Op)
                        .filter(and_(Op.graph_id == graph_id, Op.subgraph_id == subgraph_id))
                        .all()
                )
            else:
                if graph_id is not None:
                    return session.query(Op).filter(Op.graph_id == graph_id).all()
                elif subgraph_id is not None:
                    return session.query(Op).filter(Op.subgraph_id == subgraph_id).all()
                else:
                    return session.query(Op).all()

    def get_ops(self, graph_id=None, status=None):
        """
        Get a list of ops based on certain parameters
        """
        Session = self.get_session()
        with Session.begin() as session:
            if graph_id is None and status is None:
                return session.query(Op).all()
            elif graph_id is not None and status is not None:
                return (
                    session.query(Op)
                        .filter(Op.graph_id == graph_id)
                        .filter(Op.status == status)
                        .all()
                )
            else:
                if graph_id is not None:
                    return session.query(Op).filter(Op.graph_id == graph_id).all()
                elif status is not None:
                    return session.query(Op).filter(Op.status == status).all()
                else:
                    return session.query(Op).all()

    def create_client_op_mapping(self, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = ClientOpMapping()

            for key, value in kwargs.items():
                setattr(mapping, key, value)

            session.add(mapping)
            return mapping

    def update_client_op_mapping(self, client_op_mapping_id, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = session.query(ClientOpMapping).get(client_op_mapping_id)
            for key, value in kwargs.items():
                setattr(mapping, key, value)
            session.add(mapping)
            return mapping

    def find_client_op_mapping(self, client_id, op_id):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = (
                session.query(ClientOpMapping)
                    .filter(
                    ClientOpMapping.client_id == client_id, ClientOpMapping.op_id == op_id,
                )
                    .first()
            )
            return mapping

    def get_incomplete_op(self):
        Session = self.get_session()
        with Session.begin() as session:
            ops = session.query(Op).filter(Op.status == OpStatus.COMPUTING).all()

            for op in ops:
                op_mappings = op.op_mappings
                if (
                        op_mappings.filter(ClientOpMapping.status == MappingStatus.SENT).count()
                        >= 3
                        or op_mappings.filter(
                    ClientOpMapping.status == MappingStatus.COMPUTING,
                ).count()
                        >= 2
                        or op_mappings.filter(
                    ClientOpMapping.status == MappingStatus.REJECTED,
                ).count()
                        >= 5
                        or op_mappings.filter(
                    ClientOpMapping.status == MappingStatus.FAILED,
                ).count()
                        >= 3
                ):
                    continue

                return op
            return None

    def get_op_status_final(self, op_id):
        op = self.get_op(op_id=op_id)
        op_mappings = op.op_mappings
        if (
                op_mappings.filter(ClientOpMapping.status == MappingStatus.FAILED).count()
                >= 3
        ):
            return 'failed'

        return 'computing'

    def get_first_graph_op(self, graph_id):
        """
        Return the first graph op
        """
        ops = self.get_graph_ops(graph_id=graph_id)
        return ops[0]

    def get_last_graph_op(self, graph_id):
        """
        Return the last graph op
        """
        ops = self.get_graph_ops(graph_id=graph_id)
        return ops[-1]

    """
    Federated learning
    """

    def get_available_federated_clients(self):
        """
        Get a list of federated clients
        """
        Session = self.get_session()
        with Session.begin() as session:
            clients = session.query(Client).filter(Client.role == 'contributor').filter(
                Client.status == 'connected',
            ).all()
            clients_sids = [client.client_id for client in clients]
            return clients, clients_sids

    def update_federated_op(self, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            ops = (
                session.query(Op)
                    .filter(Op.operator == 'federated_training')
                    .filter(Op.id == kwargs.get('id'))
                    .all()
            )
            # print(ops)
            for op in ops:
                for key, value in kwargs.items():
                    setattr(op, key, value)
                session.add(op)

    def update_federated_client_status(self, client, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            for key, value in kwargs.items():
                setattr(client, key, value)
            session.add(client)

    """
    Graph client mapping
    """

    def create_graph_client_mapping(self, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = GraphClientMapping()

            for key, value in kwargs.items():
                setattr(mapping, key, value)

            session.add(mapping)
            return mapping

    def update_graph_client_mapping(self, graph_client_mapping_id, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = session.query(GraphClientMapping).get(graph_client_mapping_id)
            for key, value in kwargs.items():
                setattr(mapping, key, value)
            session.add(mapping)
            return mapping

    def find_graph_client_mapping(self, graph_id, client_id):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = (
                session.query(GraphClientMapping)
                    .filter(
                    GraphClientMapping.client_id == client_id,
                    GraphClientMapping.graph_id == graph_id,
                )
                    .first()
            )
            return mapping

    """
    Subgraph client mapping
    """

    def create_subgraph_client_mapping(self, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = SubgraphClientMapping()

            for key, value in kwargs.items():
                setattr(mapping, key, value)

            session.add(mapping)
            return mapping

    def update_subgraph_client_mapping(self, subgraph_client_mapping_id, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = session.query(SubgraphClientMapping).get(subgraph_client_mapping_id)
            for key, value in kwargs.items():
                setattr(mapping, key, value)
            session.add(mapping)
            return mapping

    def find_subgraph_client_mapping(self, graph_id, subgraph_id, client_id):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = (
                session.query(SubgraphClientMapping)
                    .filter(
                    SubgraphClientMapping.client_id == client_id,
                    SubgraphClientMapping.graph_id == graph_id,
                    SubgraphClientMapping.subgraph_id == subgraph_id,
                ).first()
            )
            return mapping

    def find_subgraph_client_mappings(self, graph_id=None, subgraph_id=None, client_id=None, status=None):
        """
        Get all subgraph client mappings
        :param graph_id: graph id
        :param subgraph_id: subgraph id
        :param client_id: client id
        :param status: status
        :return: mappings
        """
        Session = self.get_session()
        with Session.begin() as session:

            if graph_id is not None:
                mappings = session.query(SubgraphClientMapping).filter(SubgraphClientMapping.graph_id==graph_id)

            if subgraph_id is not None:
                mappings = mappings.filter(SubgraphClientMapping.subgraph_id == subgraph_id)

            if client_id is not None:
                mappings = mappings.filter(SubgraphClientMapping.client_id == client_id)

            if status is not None:
                mappings = mappings.filter(SubgraphClientMapping.status == status)

            return mappings.all()

    def get_subgraph_client_mappings(self, subgraph_id=None, client_id=None, status=None):
        if subgraph_id is None and client_id is None:
            return None
        Session = self.get_session()
        with Session.begin() as session:
            mappings = session.query(SubgraphClientMapping)

            if subgraph_id is not None:
                mappings = (
                    mappings
                        .filter(
                        SubgraphClientMapping.subgraph_id == subgraph_id,
                    )
                )
            elif client_id is not None:
                mappings = (
                    mappings
                        .filter(
                        SubgraphClientMapping.client_id == client_id,
                    )
                )

            if status is not None:
                mappings = (
                    mappings
                        .filter(
                        SubgraphClientMapping.status == status,
                    )
                )

            return mappings

    """
    Objective
    """

    def create_objective(self, **kwargs):
        # print(kwargs)
        return self.add('objective', **kwargs)

    def update_objective(self, objective_id, **kwargs):
        return self.update('objective', objective_id, **kwargs)

    def get_objective(self, objective_id):
        return self.get('objective', objective_id)

    def find_active_objective(self, client_id):
        Session = self.get_session()
        with Session.begin() as session:
            objectives = (
                session.query(Objective)
                    .filter(or_(Objective.status == 'pending', Objective.status == 'active'))
                    .all()
            )
            for objective in objectives:
                if self.find_objective_client_mapping(objective.id, client_id) is None:
                    return objective
            return None

    def get_objectives(self):
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Objective).all()

    def create_objective_client_mapping(self, **kwargs):
        return self.add('objective_client_mapping', **kwargs)

    def update_objective_client_mapping(self, objective_client_mapping_id, **kwargs):
        return self.update(
            'objective_client_mapping', objective_client_mapping_id, **kwargs,
        )

    def get_objective_client_mapping(self, objective_client_mapping_id):
        return self.get('objective_client_mapping', objective_client_mapping_id)

    def get_objective_client_mappings(self):
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(ObjectiveClientMapping).all()

    def find_objective_client_mapping(self, objective_id, client_id):
        Session = self.get_session()
        with Session.begin() as session:
            mapping = (
                session.query(ObjectiveClientMapping)
                    .filter(
                    ObjectiveClientMapping.client_id == client_id,
                    ObjectiveClientMapping.objective_id == objective_id,
                )
                    .first()
            )
            return mapping

    def get_objective_mappings(self, objective_id, status=None):
        Session = self.get_session()
        with Session.begin() as session:
            if status is not None:
                return session.query(ObjectiveClientMapping).filter(
                    ObjectiveClientMapping.objective_id == objective_id,
                    ObjectiveClientMapping.status == MappingStatus.COMPUTED,
                )
            else:
                return session.query(ObjectiveClientMapping).filter(
                    ObjectiveClientMapping.objective_id == objective_id,
                )

    """
    Client SID Mapping
    """

    def create_client_sid_mapping(self, **kwargs):
        return self.add('client_sid_mapping', **kwargs)

    def update_client_sid_mapping(self, client_sid_mapping_id, **kwargs):
        return self.update('client_sid_mapping', client_sid_mapping_id, **kwargs)

    def get_client_sid_mapping(self, client_sid_mapping_id):
        return self.get('client_sid_mapping', client_sid_mapping_id)

    def delete_client_sid_mapping(self, sid):
        Session = self.get_session()
        with Session.begin() as session:
            obj = session.query(ClientSIDMapping).filter(
                ClientSIDMapping.sid == sid,
            ).first()
            session.delete(obj)

    def find_client_sid_mapping(self, cid, sid):
        Session = self.get_session()
        with Session.begin() as session:
            return (
                session.query(ClientSIDMapping)
                    .filter(and_(ClientSIDMapping.sid == sid, ClientSIDMapping.cid == cid))
                    .first()
            )

    # def get_client_by_sid(self, sid):
    #     client_sid_mapping = (
    #         session.query(ClientSIDMapping)
    #         .filter(ClientSIDMapping.sid == sid)
    #         .first()
    #     )
    #     if client_sid_mapping is None:
    #         return None
    #     else:
    #         return self.get_client_by_cid(cid=client_sid_mapping.cid)

    def get_client_by_sid(self, sid):
        Session = self.get_session()
        with Session.begin() as session:
            client = session.query(Client).filter(Client.sid == sid).first()
            return client

    def get_op_output(self, op_id):
        op = self.get_op(op_id)
        if op.outputs is None or op.outputs == 'null':
            return None

        data_id = json.loads(op.outputs)[0]
        data = self.get_data(data_id=data_id)
        data = load_data_from_file(data.file_path)
        return data

    def initialize_subgraph_complexities_list(self, subgraph_dictionary):
        for subgraph_id in subgraph_dictionary:
            op_list_complexity = 0
            for op_id in subgraph_dictionary[subgraph_id]:
                # current_graph_id = self.get_op(op_id).graph_id
                op = self.get_op(op_id)
                if op.status == 'pending':
                    op_list_complexity += self.get_op(op_id).complexity
            subgraph_dictionary[subgraph_id] = op_list_complexity
        if len(subgraph_dictionary) > 0:
            subgraph_complexities_list = []
            for i in subgraph_dictionary:
                subgraph_complexities_list.append(subgraph_dictionary[i])

        # graph = self.get_graph(graph_id=current_graph_id)
        # self.update_graph(graph,subgraph=str(subgraph_dictionary))

    def get_subgraph_complexities(self, graph_id):
        graph = self.get_graph(graph_id=graph_id)
        if graph.subgraph_complexities is None:
            return None
        return graph.subgraph_complexities

    def get_subgraph_complexity(self, subgraph_id, graph_id):
        subgraph = self.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)
        ops = json.loads(subgraph.op_ids)
        complexity = 0
        for op in ops:
            op_obj = self.get_op(op_id=op)
            if op_obj.billed == 'False':
                complexity += op_obj.complexity
                self.update_op(op_obj, billed='True')

        return complexity

    def get_all_subgraphs(self, graph_id):
        """
        Get all subgraphs belonging to a graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).all()

    def get_last_10_subgraphs(self, graph_id):
        """
        Get last 10 subgraphs belonging to a graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            last_element = session.query(SubGraph).filter(SubGraph.graph_id == graph_id).order_by(
                SubGraph.id.desc(),
            ).first()
            if last_element is not None:
                if last_element.id > 10:
                    return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).order_by(
                        SubGraph.id.desc(),
                    ).limit(10)[::-1]

            return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).all()

    def get_last_10_computed_subgraphs(self, graph_id):
        """
        Get last 10 computed subgraphs belonging to a graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            last_element = session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(SubGraph.status == 'computed').order_by(
                SubGraph.id.desc()).first()
            if last_element is not None:
                if last_element.id > 10:
                    return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(SubGraph.status == 'computed').order_by(
                        SubGraph.id.desc()).limit(10)[::-1]

            return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(SubGraph.status == 'computed').all()

    def get_lin_op_data_ids(self, graph_id):
        Session = self.get_session()
        with Session.begin() as session:
            ops = session.query(Op).filter(and_(Op.graph_id == graph_id, Op.operator == 'lin')).all()
            data_file_paths = []
            for op in ops:
                data_obj = self.get_data(data_id=ast.literal_eval(op.outputs)[0])
                data_file_paths.append(data_obj.file_path)
            return data_file_paths

    def get_horizontal_split_subgraphs(self, graph_id):
        """
        Get all subgraphs belonging to a graph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(SubGraph).filter(
                and_(
                    SubGraph.graph_id == graph_id,
                    SubGraph.status != 'computed',
                    SubGraph.status != 'standby',
                    SubGraph.status != 'failed',
                    SubGraph.status != 'computing',
                    SubGraph.status != 'assigned',
                ),
            ).all()

    def get_first_ready_subgraph_from_graph(self, graph_id):
        """
        Get an existing subgraph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(SubGraph).filter(
                and_(SubGraph.graph_id == graph_id, SubGraph.status == 'ready'),
            ).first()

    def get_first_non_computed_subgraph(self, graph_id):
        """
        Get an existing subgraph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(SubGraph).filter(
                and_(SubGraph.graph_id == graph_id, SubGraph.status != 'computed'),
            ).first()

    def get_first_active_subgraph_from_graph(self, graph_id):
        """
        Get an existing subgraph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(
                or_(SubGraph.status == 'ready', SubGraph.status == 'not_ready', SubGraph.status == 'standby'),
            ).first()

    def get_ready_subgraphs_from_graph(self, graph_id):
        """
        Get existing ready subgraphs
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(
                SubGraph.status == 'ready',
            ).all()

    def get_not_ready_subgraphs_from_graph(self, graph_id):
        """
        Get existing not ready subgraphs
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(
                SubGraph.status == 'not_ready',
            ).all()

    def get_subgraphs_from_graph(self, graph_id):
        """
        Get an existing subgraph
        """
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(
                or_(
                    SubGraph.status == 'ready',
                    SubGraph.status == 'computing',
                    SubGraph.status == 'assigned',
                ),
            ).all()

    def get_subgraphs(self, graph_id=None, status=None):
        """
        Find subgraphs
        """
        Session = self.get_session()
        with Session.begin() as session:
            if status is not None and graph_id is not None:
                return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(
                        SubGraph.status == status
                ).all()
            elif graph_id is not None:
                return session.query(SubGraph).filter(SubGraph.graph_id == graph_id).all()
            elif status is not None:
                return session.query(SubGraph).filter(
                    SubGraph.status == status
                ).all()

    def get_if_failed_from_graph(self, graph_id):
        Session = self.get_session()
        with Session.begin() as session:
            return session.query(Graph).filter(Graph.id == graph_id).filter(Graph.failed_subgraph == 'True')

    def get_failed_subgraphs_from_graph(self, graph):
        """
        Get an existing subgraph
        """
        Session = self.get_session()
        with Session.begin() as session:
            graph_id = graph.id
            if graph.status != 'failed':
                # failed_ops = session.query(Op).filter(Op.graph_id == graph_id).filter(
                # Op.status == 'failed').all()
                # #, Op.message == 'values passed to tensor(values) must be a number/boolean/string
                # or an array of numbers/booleans/strings, or a TypedArray')).all()
                # unique_subgraph_ids = []
                # for failed_op in failed_ops:
                #     if failed_op.subgraph_id not in unique_subgraph_ids:
                #         unique_subgraph_ids.append(failed_op.subgraph_id)
                # return unique_subgraph_ids
                failed_subgraphs = session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(
                    SubGraph.status == 'failed',
                ).order_by(SubGraph.id.desc()).limit(30)[::-1]
                unique_subgraph_ids = []
                for failed_subgraph in failed_subgraphs:
                    if failed_subgraph.subgraph_id not in unique_subgraph_ids:
                        unique_subgraph_ids.append(failed_subgraph.subgraph_id)
                return unique_subgraph_ids
            else:
                return []

    def get_subgraph(self, subgraph_id, graph_id):
        """
        Get an existing subgraph
        """
        Session = self.get_session()
        with Session.begin() as session:
            subgraph = session.query(SubGraph).filter(
                SubGraph.subgraph_id == subgraph_id,
                SubGraph.graph_id == graph_id,
            ).first()
            return subgraph

    def delete_subgraph(self, subgraph_id, graph_id):
        Session = self.get_session()
        with Session.begin() as session:
            subgraph = self.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)
            session.delete(subgraph)

    def update_subgraph_complexity(self, subgraph_id):
        subgraph = self.get_subgraph(subgraph_id=subgraph_id)
        op_ids = ast.literal_eval(subgraph.op_ids)
        pending_ops = []
        for op in op_ids:
            op_obj = self.get_op(op)
            if op_obj.status == 'pending':
                pending_ops.append(op_obj)
        subgraph_complexity = 0
        for pending_op in pending_ops:
            subgraph_complexity += pending_op.complexity

        self.update_subgraph(subgraph, complexity=subgraph_complexity)

        if subgraph_complexity == 0:
            self.update_subgraph(subgraph, status='computed')

        # return subgraph_complexities_list

    def create_subgraph(self, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            subgraph = SubGraph()

            for key, value in kwargs.items():
                setattr(subgraph, key, value)

            session.add(subgraph)
            return subgraph

    def update_subgraph(self, subgraph, **kwargs):
        Session = self.get_session()
        with Session.begin() as session:
            for key, value in kwargs.items():
                setattr(subgraph, key, value)
            session.add(subgraph)
            return subgraph

    def get_last_active_time(self, cid):
        Session = self.get_session()
        with Session.begin() as session:
            client = session.query(Client).filter(and_(Client.cid == cid, Client.role == 'contributor')).first()
            return client.last_active_time

    def get_my_graphs(self, cid):
        """
        Get a list of graphs created by this client
        :param client: client object
        :return:
        """
        if cid is not None:
            Session = self.get_session()
            with Session.begin() as session:
                graphs = session.query(Graph)
                graphs = graphs.filter(Graph.owner == cid).all()
                return graphs
        else:
            return None
