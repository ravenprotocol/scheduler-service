from __future__ import annotations

import datetime
import time

from .globals import globals as g
from scheduler import ravdb


def update_client_status():
    while True:
        time.sleep(5)
        clients = ravdb.get_clients(status='connected')
        for client in clients:
            # To be reduced.
            if (datetime.datetime.utcnow() - client.last_active_time).seconds > 30:
                g.logger.debug(f'Disconnecting Client: {client.cid}\n')
                ravdb.update_client(
                    client, status='disconnected', reporting='ready',
                    disconnected_at=datetime.datetime.utcnow(),
                )
                assigned_subgraph = ravdb.get_subgraph(
                    client.current_subgraph_id, client.current_graph_id)
                if assigned_subgraph is not None:
                    ravdb.update_subgraph(
                        assigned_subgraph, status='ready', complexity=666)
                    subgraph_ops = ravdb.get_subgraph_ops(
                        graph_id=assigned_subgraph.graph_id,
                        subgraph_id=assigned_subgraph.subgraph_id,
                    )
                    for subgraph_op in subgraph_ops:
                        if subgraph_op.status != 'computed':
                            ravdb.update_op(subgraph_op, status='pending')
