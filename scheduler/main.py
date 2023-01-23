import ast
import json
import os
import random
import shutil
import time
import datetime
import requests
import networkx as nx

from .config import FTP_FILES_PATH, MINIMUM_SPLIT_SIZE
from .db import ravdb
from .globals import globals as g
from .strings import *

Queue = list()


def retry_failed_subgraphs(graph_id):
    global Queue
    graph = ravdb.get_graph(graph_id)
    failed_subgraph_ids = ravdb.get_failed_subgraphs_from_graph(graph)
    if len(failed_subgraph_ids) > 0:
        for failed_subgraph_id in failed_subgraph_ids:
            failed_subgraph = ravdb.get_subgraph(subgraph_id=failed_subgraph_id, graph_id=graph_id)
            assigned_client = ravdb.get_assigned_client(failed_subgraph.subgraph_id, failed_subgraph.graph_id)
            if assigned_client is not None:
                ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)

            retries = failed_subgraph.retry_attempts
            op_ids = ast.literal_eval(failed_subgraph.op_ids)
            if int(retries) <= 5:
                failed_combination = (failed_subgraph.subgraph_id, failed_subgraph.graph_id)
                if failed_combination not in Queue:
                    Queue.append(failed_combination)
                    print("\nAppended to Failed Queue: ", Queue)

            else:
                ravdb.update_subgraph(failed_subgraph, status='failed', complexity=6)
                ravdb.update_graph(graph, status="failed")
                for op_id in op_ids:
                    failed_op = ravdb.get_op(op_id)
                    ravdb.update_op(failed_op, status="failed")

def get_failed_subgraphs_from_queue(graph_id):
    failed_subgraph_ids = []
    for failed_subgraph_id, failed_graph_id in Queue:
        if failed_graph_id == graph_id:
            failed_subgraph_ids.append(failed_subgraph_id)
    return failed_subgraph_ids

def move_file(src, dest):
    try:
        shutil.move(src, dest)
    except:
        print('Error moving file')


def get_op_stats(ops):
    pending_ops = 0
    computed_ops = 0
    computing_ops = 0
    failed_ops = 0

    for op in ops:
        if op.status == 'pending':
            pending_ops += 1
        elif op.status == 'computed':
            computed_ops += 1
        elif op.status == 'computing':
            computing_ops += 1
        elif op.status == 'failed':
            failed_ops += 1

    total_ops = len(ops)

    return {
        'total_ops': total_ops,
        'pending_ops': pending_ops,
        'computing_ops': computing_ops,
        'computed_ops': computed_ops,
        'failed_ops': failed_ops,
    }

def check_graph_progress(graph_id=None):
    if graph_id is not None:
        ops = ravdb.get_graph_ops(graph_id=graph_id)
        stats = get_op_stats(ops)

        if stats["total_ops"] == 0:
            return 0
        progress = (
                           (stats["computed_ops"])
                           / stats["total_ops"]
                   ) * 100
        return progress
    else:
        return 0

def move_persisting_ops_to_developer_folder(graph_id):
    developer = ravdb.get_developer_from_graph(graph_id)
    developer_name = developer.cid
    persisting_ops = ravdb.get_persisting_ops(graph_id)
    unemitted_ops = ravdb.get_unemitted_ops(graph_id)

    for unemitted_op in unemitted_ops:
        if unemitted_op.outputs is not None:
            data_id = ast.literal_eval(unemitted_op.outputs)[0]
            data_obj = ravdb.get_data(data_id)
            file_path = data_obj.file_path
            os.remove(file_path)

    for op in persisting_ops:
        data_id = ast.literal_eval(op.outputs)[0]
        data_obj = ravdb.get_data(data_id)
        src = data_obj.file_path
        dst = FTP_FILES_PATH + '/' + developer_name + "/" + "data_{}.pkl".format(data_id)
        move_file(src, dst)
        ravdb.update_data(data_obj, file_path=dst)

def vertical_split(graph_id, minimum_split_size):
    op_dependency = ravdb.get_graph_op_dependency(graph_id, minimum_split_size=minimum_split_size)

    # print('\n\nOP DEPENDENCY: {}'.format(op_dependency))

    if len(op_dependency) == 0:
        progress = int(check_graph_progress(graph_id))
        if progress == 100:
            graph = ravdb.get_graph(graph_id=graph_id)
            ravdb.update_graph(graph, status=GraphStatus.COMPUTED)
            move_persisting_ops_to_developer_folder(graph_id)

    for subgraph_id in op_dependency:
        op_ids = op_dependency[subgraph_id]
        for op_id in op_ids:
            op = ravdb.get_op(op_id)
            ravdb.update_op(op, subgraph_id=subgraph_id)

        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)

        subgraph_ops = ravdb.get_subgraph_ops(graph_id=graph_id, subgraph_id=subgraph_id)
        subgraph_op_ids = []
        for subgraph_op in subgraph_ops:
            subgraph_op_ids.append(subgraph_op.id)
        subgraph_op_ids.sort()

        if subgraph is None:
            if len(op_dependency.keys()) <= 1:
                subgraph = ravdb.create_subgraph(subgraph_id=subgraph_id, graph_id=graph_id,
                                                 op_ids=str(subgraph_op_ids), status=SubgraphStatus.READY, complexity=1)
        else:
            if subgraph.status != 'failed':
                if subgraph.status == 'standby':
                    parent_subgraph = ravdb.get_subgraph(subgraph_id=subgraph.parent_subgraph_id, graph_id=graph_id)
                    if parent_subgraph.status == SubgraphStatus.COMPUTED:
                        ravdb.update_subgraph(subgraph, op_ids=str(subgraph_op_ids), status='not_ready', complexity=1)
                    else:
                        standby_ops_ids = ast.literal_eval(subgraph.op_ids)
                        standby_flag = False
                        for standby_op_id in standby_ops_ids:
                            standby_op = ravdb.get_op(standby_op_id)
                            if standby_op.inputs != 'null':
                                standby_op_inputs = ast.literal_eval(standby_op.inputs)
                                for standby_op_input_id in standby_op_inputs:
                                    standby_op_input = ravdb.get_op(standby_op_input_id)
                                    if standby_op_input.subgraph_id != subgraph_id:
                                        if standby_op_input.status != 'computed':
                                            standby_flag = True
                                            break
                                if standby_flag:
                                    break
                        if not standby_flag:
                            ravdb.update_subgraph(subgraph, op_ids=str(subgraph_op_ids), status='not_ready',
                                                  complexity=1)
                else:
                    if subgraph.status == "computed":
                        assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                        if assigned_client is not None:
                            ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None,
                                                current_graph_id=None)

                        ravdb.update_subgraph(subgraph, status="not_ready", optimized="False",
                                              op_ids=str(subgraph_op_ids), retry_attempts=0, complexity=2)

                    if len(op_ids) != 0:
                        if subgraph.status != 'assigned' and subgraph.status != 'computing':
                            ravdb.update_subgraph(subgraph, op_ids=str(subgraph_op_ids), complexity=3)

        break

    last_id = len(ravdb.get_all_subgraphs(graph_id=graph_id))
    if last_id == 0:
        last_id = 1
    new_op_dependency = {}
    for subgraph_id in op_dependency:
        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)
        if subgraph is not None and subgraph.status != 'standby' and subgraph.status != 'computed' and subgraph.status != 'computing' and subgraph.status != "assigned" and subgraph.status != "failed":
            if subgraph.optimized == "False":
                G = nx.DiGraph()
                op_ids = ast.literal_eval(subgraph.op_ids)
                miscellaneous_ops = []
                for op_id in op_ids:
                    op = ravdb.get_op(op_id)
                    if op.status != "computed":
                        if op.inputs != "null":
                            inputs_computed_flag = True
                            for input_id in ast.literal_eval(op.inputs):
                                input_op = ravdb.get_op(input_id)
                                if input_op.status != "computed" and input_op.status != "computing":
                                    G.add_edge(input_id, op_id)
                                    inputs_computed_flag = False
                            if inputs_computed_flag:
                                if op.dependents is not None:
                                    dependent_ops = ast.literal_eval(op.dependents)
                                    same_subgraph_flag = False
                                    for dependent_op_id in dependent_ops:
                                        dependent_op = ravdb.get_op(dependent_op_id)
                                        if dependent_op.subgraph_id == op.subgraph_id:
                                            same_subgraph_flag = True
                                            break
                                    if not same_subgraph_flag:
                                        miscellaneous_ops.append(op_id)
                                else:
                                    miscellaneous_ops.append(op_id)

                    for key, value in json.loads(op.params).items():
                        if type(value).__name__ == "int":
                            op1 = ravdb.get_op(value)
                            if op1.status != "computed" and op1.status != "computing":
                                G.add_edge(value, op_id)

                subsubgraphs = list(nx.weakly_connected_components(G))
                subsubgraphs = [list(x) for x in subsubgraphs]

                if len(subsubgraphs) > 1:
                    new_op_dependency[subgraph_id] = subsubgraphs[0]
                    for i in range(1, len(subsubgraphs)):
                        new_op_dependency[last_id + i] = subsubgraphs[i]
                elif len(subsubgraphs) == 1:
                    new_op_dependency[subgraph_id] = subsubgraphs[0]

                if len(new_op_dependency) != 0:
                    last_id = list(new_op_dependency.keys())[-1]

                for op_id in miscellaneous_ops:
                    if len(new_op_dependency) == 0:
                        new_op_dependency[subgraph_id] = [op_id]
                    else:
                        new_op_dependency[list(new_op_dependency.keys())[-1]].append(op_id)

    for subgraph_id in new_op_dependency:
        op_ids = new_op_dependency[subgraph_id]
        for k in range(len(op_ids)):
            op = ravdb.get_op(op_ids[k])
            ravdb.update_op(op, subgraph_id=subgraph_id)

        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)
        sorted_new_op_deps = op_ids
        sorted_new_op_deps.sort()

        if subgraph is not None:            
            ravdb.update_subgraph(subgraph, subgraph_id=subgraph_id, graph_id=graph_id,
                                  op_ids=str(sorted_new_op_deps),
                                  optimized="True", status=SubgraphStatus.READY, complexity=4)
        else:
            subgraph = ravdb.create_subgraph(subgraph_id=subgraph_id, graph_id=graph_id,
                                             op_ids=str(sorted_new_op_deps), status=SubgraphStatus.READY,
                                             optimized="True", complexity=2)


def horizontal_split(graph_id, minimum_split_size=MINIMUM_SPLIT_SIZE):
    subgraphs = ravdb.get_horizontal_split_subgraphs(graph_id=graph_id)
    for subgraph in subgraphs:
        if subgraph.has_failed == "False" and int(
                subgraph.retry_attempts) <= 1:  
            op_ids = ast.literal_eval(subgraph.op_ids)
            if len(op_ids) > minimum_split_size:
                row1 = op_ids[:minimum_split_size]
                parent_subgraph = ravdb.get_subgraph(subgraph_id=subgraph.parent_subgraph_id, graph_id=graph_id)
                if parent_subgraph is not None:
                    if str(row1) == str(parent_subgraph.op_ids):
                        minimum_split_size += random.randint(1, len(op_ids) - minimum_split_size)
                        row1 = op_ids[:minimum_split_size]

                row2 = op_ids[minimum_split_size:]
                ravdb.update_subgraph(subgraph, op_ids=str(row1), complexity=5)
                last_subgraph_id = len(ravdb.get_all_subgraphs(graph_id=graph_id))
                if len(row2) > 0:
                    new_subgraph = ravdb.create_subgraph(subgraph_id=last_subgraph_id + 1, graph_id=graph_id,
                                                         optimized="False", op_ids=str(row2), status="standby",
                                                         parent_subgraph_id=subgraph.subgraph_id, complexity=3)
                    for op_id in row2:
                        op = ravdb.get_op(op_id)
                        ravdb.update_op(op, subgraph_id=new_subgraph.subgraph_id)

def calculate_subgraph_complexity(subgraph):
    op_ids = ast.literal_eval(subgraph.op_ids)
    pending_ops = []
    for op in op_ids:
        op_obj = ravdb.get_op(op)
        if op_obj.status == "pending":
            pending_ops.append(op_obj)
    subgraph_complexity = 0
    for pending_op in pending_ops:
        subgraph_complexity += pending_op.complexity
    return subgraph_complexity

def create_sub_graphs(graph_id):
    op_dependency = ravdb.get_graph_op_dependency(graph_id)
    # print('OP DEPENDENCY: ',op_dependency)
    for subgraph_id in op_dependency:
        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)

        if subgraph is not None:
            complexity = calculate_subgraph_complexity(subgraph=subgraph)
            ravdb.update_subgraph(subgraph, subgraph_id=subgraph_id, graph_id=graph_id,
                                  op_ids=str(op_dependency[subgraph_id]),
                                  complexity=complexity)
        else:
            subgraph = ravdb.create_subgraph(subgraph_id=subgraph_id, graph_id=graph_id,
                                             op_ids=str(op_dependency[subgraph_id]), status=SubgraphStatus.READY)
            complexity = calculate_subgraph_complexity(subgraph=subgraph)
            ravdb.update_subgraph(subgraph, complexity=complexity)

def run_scheduler():
    global Queue
    g.logger.debug("Scheduler started...")
    c = 0
    while True:
        if c % 10 == 0:
            update_client_status()
            c = 0
        forward_distributed_graphs = ravdb.get_graphs(status=GraphStatus.PENDING, approach="distributed", execute="True", computation_mode="forward")
        federated_graphs = ravdb.get_graphs(status=GraphStatus.PENDING, approach="federated", execute="True")

        if len(forward_distributed_graphs) == 0 and len(federated_graphs) == 0:
            pass

        else:
            for federated_graph in federated_graphs:
                created_subgraphs = ravdb.get_subgraphs_from_graph(federated_graph.id)
                if len(created_subgraphs) == 0:
                    create_sub_graphs(federated_graph.id)

            for forward_distributed_graph in forward_distributed_graphs:
                current_graph_id = forward_distributed_graph.id
                ravdb.update_graph(forward_distributed_graph, inactivity=int(forward_distributed_graph.inactivity) + 1)

                split_dead_subgraphs(forward_distributed_graph, current_graph_id)
                handle_failed_subgraphs(forward_distributed_graph, current_graph_id)
                assign_subgraphs_to_clients(current_graph_id)
                pop_failed_subgraphs_from_queue(current_graph_id)
                
        time.sleep(0.1)
        c += 1

def split_dead_subgraphs(distributed_graph, current_graph_id):
    dead_subgraph = ravdb.get_first_ready_subgraph_from_graph(graph_id=current_graph_id)
    if dead_subgraph is None:
        vertical_split(distributed_graph.id, minimum_split_size=distributed_graph.min_split_size)
        # horizontal_split(distributed_graph.id, minimum_split_size=distributed_graph.min_split_size)
    elif dead_subgraph is not None and dead_subgraph.optimized == "False":
        vertical_split(distributed_graph.id, minimum_split_size=distributed_graph.min_split_size)
        # horizontal_split(distributed_graph.id, minimum_split_size=distributed_graph.min_split_size)

def handle_failed_subgraphs(distributed_graph, current_graph_id):
    if_failed_subgraph = ravdb.get_if_failed_from_graph(distributed_graph.id)
    if len(if_failed_subgraph) > 0:# is not None:
        g.logger.debug("Failed Subgraph: {}".format(if_failed_subgraph))
        retry_failed_subgraphs(distributed_graph.id)
        ravdb.update_graph(distributed_graph, failed_subgraph="False")

    failed_subgraph_ids = get_failed_subgraphs_from_queue(current_graph_id)
    for subgraph_id in failed_subgraph_ids:
        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=current_graph_id)
        if subgraph.status != "assigned" and subgraph.status != "computing" and subgraph.status != "computed":
            if subgraph.has_failed != "True" or subgraph.status == "failed":
                op_ids = ast.literal_eval(subgraph.op_ids)
                for op_id in op_ids:
                    failed_op = ravdb.get_op(op_id)
                    if failed_op.status != "computed":
                        if failed_op.operator != "lin" and failed_op.status != "computed":
                            ravdb.update_op(failed_op, subgraph_id=subgraph_id, message=None, status="pending")
                        elif failed_op.operator == "lin":
                            ravdb.update_op(failed_op, subgraph_id=subgraph_id, message=None)
                    
                ravdb.update_subgraph(subgraph, status='ready', optimized='True', has_failed="True", complexity=14)

def assign_subgraphs_to_clients(current_graph_id):
    subgraphs = ravdb.get_ready_subgraphs_from_graph(graph_id=current_graph_id)
    for subgraph in subgraphs:
        ready_flag = True
        op_ids = ast.literal_eval(subgraph.op_ids)
        for op_id in op_ids:
            op = ravdb.get_op(op_id)
            if op.status == "computing":
                ready_flag = False
                break
            if op.inputs != 'null':
                for input_op_id in ast.literal_eval(op.inputs):
                    input_op = ravdb.get_op(input_op_id)
                    if input_op.subgraph_id != subgraph.subgraph_id:
                        if input_op.status != "computed":
                            ready_flag = False
                            break
                if not ready_flag:
                    break
            for key, value in json.loads(op.params).items():
                if type(value).__name__ == "int":
                    op1 = ravdb.get_op(value)
                    if op1.subgraph_id != subgraph.subgraph_id:
                        if op1.status != "computed" and "backward_pass_" not in op1.operator:
                            ready_flag = False
                            break
            if not ready_flag:
                break

        if not ready_flag:
            continue

        idle_clients = ravdb.get_idle_clients(reporting=ClientStatus.IDLE)
        if idle_clients is not None:
            op_ids = ast.literal_eval(subgraph.op_ids)
            prelim_times = {}
            for idle_client in idle_clients:
                idle_client_time = 0
                for op_id in op_ids:
                    op = ravdb.get_op(op_id=op_id)
                    if op is not None:
                        operator = op.operator
                        capabilities_dict = ast.literal_eval(idle_client.capabilities)
                        if operator not in capabilities_dict.keys():
                            client_time = random.random() * 10
                        else:
                            client_time = capabilities_dict[operator]
                        idle_client_time += client_time
                prelim_times[idle_client.id] = idle_client_time
            if bool(prelim_times):
                previously_assigned_client = ravdb.get_assigned_client(subgraph_id=subgraph.subgraph_id,
                                                                        graph_id=subgraph.graph_id)
                if previously_assigned_client is not None:
                    ravdb.update_subgraph(subgraph, status='assigned', complexity=150)
                else:
                    fastest_client_id = min(prelim_times, key=prelim_times.get)
                    client = ravdb.get_client(id=fastest_client_id)
                    ravdb.update_subgraph(subgraph, status='assigned', complexity=15)
                    ravdb.update_client(client, reporting='busy', current_subgraph_id=subgraph.subgraph_id,
                                        current_graph_id=subgraph.graph_id)
                    g.logger.debug("Assigned Subgraph ID: {} Graph ID: {} to Client: {}".format(subgraph.subgraph_id, subgraph.graph_id, client.cid))
                res = requests.get("http://localhost:8081/comms/assigned/?cid={}&subgraph_id={}&graph_id={}&mode=0".format(client.cid, subgraph.subgraph_id, subgraph.graph_id))
                if res.status_code == 300:
                    ravdb.update_subgraph(subgraph, status='ready', complexity=15)
                    g.logger.debug("\nFailed to assign Subgraph ID: {} Graph ID: {} to Client: {}".format(subgraph.subgraph_id, subgraph.graph_id, client.cid))

def pop_failed_subgraphs_from_queue(current_graph_id):
    global Queue
    subgraphs = ravdb.get_last_10_computed_subgraphs(graph_id=current_graph_id)
    if len(subgraphs) > 10:
        subgraphs = subgraphs[-10:]
    for subgraph in subgraphs:
        if subgraph.has_failed == "True":
            temp_Queue = Queue
            for queue_subgraph_id, queue_graph_id in Queue:
                if queue_subgraph_id == subgraph.subgraph_id and queue_graph_id == current_graph_id:
                    temp_Queue.remove((queue_subgraph_id, queue_graph_id))
                    print("\nPopped from Failed Queue: ", (queue_subgraph_id, queue_graph_id))
            Queue = temp_Queue

def update_client_status():
    clients = ravdb.get_idle_connected_clients(status='connected')
    for client in clients:
        current_time = datetime.datetime.utcnow()
        last_active_time = ravdb.get_last_active_time(client.cid)
        if current_time > last_active_time:
            time_difference = (current_time - last_active_time).seconds
        else:
            time_difference = (last_active_time - current_time).seconds

        if time_difference > 30:  
            g.logger.debug("Exceeded 30 seconds")
            disconnect_client(client)


def disconnect_client(client):
    g.logger.debug('Disconnecting Client: {}\n'.format(client.cid))
    ravdb.update_client(client, status="disconnected", reporting='ready', disconnected_at=datetime.datetime.utcnow())
    assigned_subgraph = ravdb.get_subgraph(client.current_subgraph_id, client.current_graph_id)
    if assigned_subgraph is not None:
        if assigned_subgraph.retry_attempts > 0:
            ravdb.update_subgraph(assigned_subgraph, status="failed", has_failed="False", retry_attempts=assigned_subgraph.retry_attempts - 1, complexity=666)
        else:
            ravdb.update_subgraph(assigned_subgraph, status="failed", has_failed="False", complexity=666)

        graph = ravdb.get_graph(graph_id=assigned_subgraph.graph_id)
        ravdb.update_graph(graph, failed_subgraph="True")
        current_subgraph_id = client.current_subgraph_id
        current_graph_id = client.current_graph_id
        ravdb.update_client(client, current_subgraph_id=None, current_graph_id=None)

        zip_file_path = FTP_FILES_PATH + '/' + str(client.cid) + '/zip_{}_{}.zip'.format(
            current_subgraph_id,
            current_graph_id,
        )

        if os.path.exists(zip_file_path):
            os.remove(zip_file_path)

        local_zip_file_path = FTP_FILES_PATH + '/' + str(client.cid) + '/local_{}_{}.zip'.format(
            current_subgraph_id,
            current_graph_id,
        )

        if os.path.exists(local_zip_file_path):
            os.remove(local_zip_file_path)

if __name__ == '__main__':
    run_scheduler()
