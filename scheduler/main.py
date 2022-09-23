import ast
import json
import os
import random
import shutil
import time

import networkx as nx

from .config import FTP_FILES_PATH, MINIMUM_SPLIT_SIZE
from .db import ravdb
from .globals import globals as g
from .strings import *

Queue = list()


def retry_failed_subgraphs(graph_id):
    graph = ravdb.get_graph(graph_id)
    failed_subgraph_ids = ravdb.get_failed_subgraphs_from_graph(graph)
    if len(failed_subgraph_ids) > 0:
        print("\nFailed subgraph ids Retry: ", failed_subgraph_ids)
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
        print("Error moving file")


def get_op_stats(ops):
    pending_ops = 0
    computed_ops = 0
    computing_ops = 0
    failed_ops = 0

    for op in ops:
        if op.status == "pending":
            pending_ops += 1
        elif op.status == "computed":
            computed_ops += 1
        elif op.status == "computing":
            computing_ops += 1
        elif op.status == "failed":
            failed_ops += 1

    total_ops = len(ops)

    return {
        "total_ops": total_ops,
        "pending_ops": pending_ops,
        "computing_ops": computing_ops,
        "computed_ops": computed_ops,
        "failed_ops": failed_ops,
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

    # print('\n\nOP DEPENDENCY: ',op_dependency)

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
                # ready_subgraph = ravdb.get_first_active_subgraph_from_graph(graph_id=graph_id)
                # if ready_subgraph is None:
                subgraph = ravdb.create_subgraph(subgraph_id=subgraph_id, graph_id=graph_id,
                                                 op_ids=str(subgraph_op_ids), status=SubgraphStatus.READY, complexity=1)
        else:
            if subgraph.status != 'failed':
                if subgraph.status == 'standby':
                    parent_subgraph = ravdb.get_subgraph(subgraph_id=subgraph.parent_subgraph_id, graph_id=graph_id)
                    # if parent_subgraph.status == SubgraphStatus.COMPUTED:# or parent_subgraph.status == SubgraphStatus.COMPUTING:
                    #     ravdb.update_subgraph(subgraph, op_ids=str(subgraph_op_ids), status='not_ready')
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
                        if not standby_flag:  # or parent_subgraph.status == SubgraphStatus.COMPUTED:
                            # if parent_subgraph.status == SubgraphStatus.COMPUTED:
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

                    # op_ids = subgraph.op_ids
                    # if len(subgraph_op_ids) == 0:
                    #     ravdb.update_subgraph(subgraph, op_ids=str(subgraph_op_ids), status='computed', optimized='True')
                    #     assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                    #     if assigned_client is not None:
                    # ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
                    # else:
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
        if subgraph is not None and subgraph.status != 'standby' and subgraph.status != 'computed' and subgraph.status != 'computing' and subgraph.status != "assigned":
            if subgraph.optimized == "False":
                computed_ops = []
                G = nx.DiGraph()

                op_ids = ast.literal_eval(subgraph.op_ids)

                for op_id in op_ids:
                    op = ravdb.get_op(op_id)
                    if op.inputs != "null":
                        for input_id in ast.literal_eval(op.inputs):
                            input_op = ravdb.get_op(input_id)
                            if input_id in computed_ops:
                                name = "ghost_op_" + str(input_id)
                                ghost_op = ravdb.create_op(name=name, graph_id=input_op.graph_id,
                                                           subgraph_id=subgraph_id,
                                                           complexity=0, inputs="null",
                                                           outputs=input_op.outputs, node_type="input", op_type="other",
                                                           operator="lin", status="computed", params=input_op.params)
                                op_inputs = ast.literal_eval(op.inputs)
                                for j in range(len(op_inputs)):
                                    if op_inputs[j] == input_id:
                                        op_inputs[j] = ghost_op.id
                                ravdb.update_op(op, inputs=str(op_inputs))
                            else:
                                if input_op.status == "computed":
                                    computed_ops.append(input_id)

                for op_id in op_ids:
                    op = ravdb.get_op(op_id)
                    if op.status != "computed":
                        if op.inputs != "null":
                            for input_id in ast.literal_eval(op.inputs):
                                G.add_edge(input_id, op_id)

                    for key, value in json.loads(op.params).items():
                        if type(value).__name__ == "int":
                            op1 = ravdb.get_op(value)
                            if op1.status != "computed":
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
            # break

    # print('\n\nNEW OP DEPENDENCY: ',new_op_dependency)
    for subgraph_id in new_op_dependency:
        op_ids = new_op_dependency[subgraph_id]
        for k in range(len(op_ids)):
            op = ravdb.get_op(op_ids[k])
            ravdb.update_op(op, subgraph_id=subgraph_id)

        # for subgraph_id in new_op_dependency:
        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)

        # sorted_new_op_deps = new_op_dependency[subgraph_id]
        sorted_new_op_deps = op_ids

        sorted_new_op_deps.sort()
        if subgraph is not None:
            # complexity = calculate_subgraph_complexity(subgraph=subgraph)
            ravdb.update_subgraph(subgraph, subgraph_id=subgraph_id, graph_id=graph_id,
                                  op_ids=str(sorted_new_op_deps),
                                  optimized="True", status=SubgraphStatus.READY, complexity=4)
        else:
            subgraph = ravdb.create_subgraph(subgraph_id=subgraph_id, graph_id=graph_id,
                                             op_ids=str(sorted_new_op_deps), status=SubgraphStatus.READY,
                                             optimized="True", complexity=2)
            # complexity = calculate_subgraph_complexity(subgraph=subgraph)
            # ravdb.update_subgraph(subgraph, complexity=2)


def horizontal_split(graph_id, minimum_split_size=MINIMUM_SPLIT_SIZE):
    # subgraphs = ravdb.get_all_subgraphs(graph_id=graph_id)
    subgraphs = ravdb.get_horizontal_split_subgraphs(graph_id=graph_id)
    for subgraph in subgraphs:
        if subgraph.has_failed == "False" and int(
                subgraph.retry_attempts) <= 1:  # and subgraph.status != SubgraphStatus.COMPUTED and subgraph.status != 'standby' and subgraph.status != 'failed' and subgraph.status != 'computing':
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
    while True:
        # print("Scheduler Running...")
        distributed_graphs = ravdb.get_graphs(status=GraphStatus.PENDING, approach="distributed", execute="True")
        federated_graphs = ravdb.get_graphs(status=GraphStatus.PENDING, approach="federated", execute="True")
        # g.logger.debug("Graphs: {} {}".format(len(distributed_graphs), len(federated_graphs)))
        if len(distributed_graphs) == 0 and len(federated_graphs) == 0:
            # print("No graphs found")
            pass

        else:
            for federated_graph in federated_graphs:
                created_subgraphs = ravdb.get_subgraphs_from_graph(federated_graph.id)
                if len(created_subgraphs) == 0:
                    create_sub_graphs(federated_graph.id)

            for distributed_graph in distributed_graphs:
                current_graph_id = distributed_graph.id
                ravdb.update_graph(distributed_graph, inactivity=int(distributed_graph.inactivity) + 1)

                dead_subgraph = ravdb.get_first_ready_subgraph_from_graph(graph_id=current_graph_id)
                if dead_subgraph is None:
                    vertical_split(distributed_graph.id, minimum_split_size=distributed_graph.min_split_size)
                    horizontal_split(distributed_graph.id, minimum_split_size=distributed_graph.min_split_size)
                elif dead_subgraph is not None and dead_subgraph.optimized == "False":
                    vertical_split(distributed_graph.id, minimum_split_size=distributed_graph.min_split_size)
                    horizontal_split(distributed_graph.id, minimum_split_size=distributed_graph.min_split_size)

                if_failed_subgraph = ravdb.get_if_failed_from_graph(distributed_graph.id)
                if if_failed_subgraph is not None:
                    retry_failed_subgraphs(distributed_graph.id)
                    ravdb.update_graph(distributed_graph, failed_subgraph="False")

                failed_subgraph_ids = get_failed_subgraphs_from_queue(current_graph_id)

                for subgraph_id in failed_subgraph_ids:
                    subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=current_graph_id)

                    if subgraph.status != "assigned" and subgraph.status != "computing" and subgraph.status != "computed":
                        if subgraph.has_failed != "True" or subgraph.status == "failed":
                            op_ids = ast.literal_eval(subgraph.op_ids)
                            final_subsubgraph_list = []
                            for op_id in op_ids:
                                op = ravdb.get_op(op_id)
                                if op.status != "computed":
                                    if op.inputs != "null":
                                        for input_op_id in ast.literal_eval(op.inputs):
                                            input_op = ravdb.get_op(input_op_id)
                                            if input_op.status != "computed":
                                                final_subsubgraph_list.append(input_op_id)
                                    final_subsubgraph_list.append(op_id)

                            final_subsubgraph_list = list(set(final_subsubgraph_list))

                            failed_ops = final_subsubgraph_list
                            failed_ops.sort()

                            for failed_op_id in failed_ops:
                                failed_op = ravdb.get_op(failed_op_id)
                                if failed_op.operator != "lin" and failed_op.status != "computed":
                                    ravdb.update_op(failed_op, subgraph_id=subgraph_id, message=None, status="pending")
                                elif failed_op.operator == "lin":
                                    ravdb.update_op(failed_op, subgraph_id=subgraph_id, message=None)

                            updated_subgraph = ravdb.update_subgraph(subgraph, op_ids=str(failed_ops), status='ready',
                                                                     optimized='True', has_failed="True", complexity=14)

                subgraphs = ravdb.get_ready_subgraphs_from_graph(graph_id=current_graph_id)

                for subgraph in subgraphs:

                    ready_flag = True
                    op_ids = ast.literal_eval(subgraph.op_ids)
                    for op_id in op_ids:
                        op = ravdb.get_op(op_id)
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
                                if op1.status != "computed":
                                    param_op = op1
                                    if param_op.subgraph_id != subgraph.subgraph_id:
                                        ravdb.update_subgraph(subgraph, parent_subgraph_id=param_op.subgraph_id,
                                                              status="standby", optimized="False")
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

                        # else:
                        # print('\n\nNo idle clients')
                        # print('')
                    # else:
                    # print('\nNo idle clients')
                    # print('')

                subgraphs = ravdb.get_last_10_subgraphs(graph_id=current_graph_id)
                if len(subgraphs) > 10:
                    subgraphs = subgraphs[-10:]
                for subgraph in subgraphs:
                    subgraph_op_ids = ast.literal_eval(subgraph.op_ids)
                    actual_op_ids = []
                    for op_id in subgraph_op_ids:
                        op = ravdb.get_op(op_id)
                        if op.subgraph_id == subgraph.subgraph_id:
                            actual_op_ids.append(op)

                    num_ops = len(actual_op_ids)
                    counter = {'pending': 0, 'computed': 0, 'failed': 0, 'computing': 0}
                    for subgraph_op in actual_op_ids:
                        if subgraph_op.status == "pending":
                            counter['pending'] += 1
                        elif subgraph_op.status == "computed":
                            counter['computed'] += 1
                        elif subgraph_op.status == "failed":
                            counter['failed'] += 1
                        elif subgraph_op.status == "computing":
                            counter['computing'] += 1

                    if subgraph.status != 'computed':
                        if counter['computed'] == num_ops:
                            ravdb.update_subgraph(subgraph, status="computed", complexity=16)
                            assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                            if assigned_client is not None:
                                ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None,
                                                    current_graph_id=None)


                        elif counter['pending'] == 0 and counter['computing'] == 0 and counter['failed'] == 0 and \
                                counter['computed'] == 0:
                            ravdb.update_subgraph(subgraph, status="computed", complexity=17)
                            assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                            if assigned_client is not None:
                                ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None,
                                                    current_graph_id=None)

                    elif subgraph.status == "computed" and subgraph.has_failed == "True":  # and int(subgraph.retry_attempts) >= 2:
                        temp_Queue = Queue
                        for queue_subgraph_id, queue_graph_id in Queue:
                            if queue_subgraph_id == subgraph.subgraph_id and queue_graph_id == current_graph_id:
                                temp_Queue.remove((queue_subgraph_id, queue_graph_id))
                                print("\nPopped from Failed Queue: ", (queue_subgraph_id, queue_graph_id))
                        Queue = temp_Queue

                    # Add failed and pending case
                    if subgraph.status == "failed" and counter['pending'] > 0:
                        assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                        if assigned_client is not None:
                            ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None,
                                                current_graph_id=None)
                        ravdb.update_subgraph(subgraph, status="not_ready", optimized="False", complexity=19)

        time.sleep(0.1)


if __name__ == '__main__':
    run_scheduler()
