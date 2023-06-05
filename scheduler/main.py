import ast
import json
import os
import random
import shutil
import time
import datetime
import requests
import networkx as nx

from .config import FTP_FILES_PATH, MINIMUM_SPLIT_SIZE, RAVENAUTH_WALLET
from .utils import verify_and_get_access_token
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
                ravdb.update_subgraph(failed_subgraph, status='failed')
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
        if op.persist == "True":
            dst = FTP_FILES_PATH + '/' + developer_name + "/" + "data_{}.pkl".format(data_id)
            move_file(src, dst)
            ravdb.update_data(data_obj, file_path=dst)
        else: #save_model
            if op.operator == "forward_pass":
                model_src = src.split('/')[:-1]
                model_src = "/".join(model_src) + '/' + 'model_{}.pt'.format(data_id)
                if os.path.exists(model_src):
                    model_dst = FTP_FILES_PATH + '/' + developer_name + "/" + "model_{}.pt".format(data_id)
                    move_file(model_src, model_dst)
                    ravdb.update_data(data_obj, file_path=model_dst)

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
            c = 0
        # forward_distributed_graphs = ravdb.get_graphs(status=GraphStatus.PENDING, approach="distributed", execute="True")
        forward_distributed_graphs = ravdb.get_executable_graphs(status=GraphStatus.PENDING, approach="distributed", execute="True")
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

                if forward_distributed_graph.proportioned != "True":
                    proportion_graph(current_graph_id, lowest_stake=0.25)
                    if forward_distributed_graph.started == "False":
                        ravdb.update_graph(forward_distributed_graph, proportioned = "True", started = "True")
                    else:
                        ravdb.update_graph(forward_distributed_graph, proportioned = "True")
                else:
                    if forward_distributed_graph.started == "False":
                        ravdb.update_graph(forward_distributed_graph, started = "True")
                    else:
                        ravdb.update_graph(forward_distributed_graph)

                # handle_failed_subgraphs(forward_distributed_graph, current_graph_id)
                assign_subgraphs_to_clients(forward_distributed_graph, current_graph_id)
                # pop_failed_subgraphs_from_queue(current_graph_id)

        time.sleep(0.1)
        c += 1

def handle_failed_subgraphs(distributed_graph, current_graph_id):
    if_failed_subgraph = ravdb.get_if_failed_from_graph(distributed_graph.id)
    if len(if_failed_subgraph) > 0:# is not None:
        g.logger.debug("Failed Subgraph: {}".format(if_failed_subgraph))
        retry_failed_subgraphs(distributed_graph.id)
        ravdb.update_graph(distributed_graph, failed_subgraph="False")

    failed_subgraph_ids = get_failed_subgraphs_from_queue(current_graph_id)
    for subgraph_id in failed_subgraph_ids:
        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=current_graph_id)
        if subgraph.status != "assigned" and subgraph.status != "computing" and subgraph.status != "computed" and subgraph.status != "hold":
            if subgraph.has_failed != "True" or subgraph.status == "failed":
                op_ids = ast.literal_eval(subgraph.op_ids)
                for op_id in op_ids:
                    failed_op = ravdb.get_op(op_id)
                    if failed_op.status != "computed":
                        if failed_op.operator != "lin" and failed_op.status != "computed":
                            ravdb.update_op(failed_op, subgraph_id=subgraph_id, message=None, status="pending")
                        elif failed_op.operator == "lin":
                            ravdb.update_op(failed_op, subgraph_id=subgraph_id, message=None)
                    
                ravdb.update_subgraph(subgraph, status='ready', optimized='True', has_failed="True")

def proportion_graph(graph_id, lowest_stake):
    affiliated_clients = ravdb.get_affiliated_clients(affiliated_graph_id=graph_id)
    num_subgraphs = ravdb.get_num_subgraphs(graph_id=graph_id)

    if num_subgraphs > 0 and len(affiliated_clients) > 0:
        # temp = num_subgraphs
        # stake_pool = 0
        # stakes = []
        # for affiliated_client in affiliated_clients:
        #     stakes.append(affiliated_client.stake)

        # proportions = [0] * len(stakes)

        # for i in range(len(stakes)):
        #     if stakes[i] <= lowest_stake:
        #         proportions[i] = 1
        #         temp -= 1

        # total = 0

        # for i in stakes:
        #     if i > lowest_stake:
        #         total += i

        # proportionality_factor = temp / total

        # for i in range(len(stakes)):
        #     if proportions[i] != 1:
        #         proportions[i] = round(proportionality_factor * stakes[i])

        # sum_proportions = sum(proportions)

        # if sum_proportions < num_subgraphs:
        #     proportions[proportions.index(max(proportions))] += num_subgraphs - sum_proportions
        # elif sum_proportions > num_subgraphs:
        #     proportions[proportions.index(max(proportions))] -= num_subgraphs - sum_proportions

        for i in range(len(affiliated_clients)):
            ravdb.update_client(affiliated_clients[i], proportion = num_subgraphs, original_proportion = num_subgraphs) #proportion = proportions[i], original_proportion = proportions[i])

def assign_subgraphs_to_clients(graph, current_graph_id):
    flag = ravdb.assignment_check(current_graph_id)
    if flag:        
        subgraph = ravdb.get_first_ready_subgraph_from_graph(graph_id=current_graph_id)
        if subgraph is not None:
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
            
            if ready_flag:
                
                idle_clients = ravdb.get_idle_clients(reporting=ClientStatus.IDLE, affiliated_graph_id=current_graph_id)
                if len(idle_clients) == graph.active_participants:
                    # previously_assigned_client = ravdb.get_assigned_client(subgraph_id=subgraph.subgraph_id,
                    #                                                         graph_id=subgraph.graph_id)
                    # if previously_assigned_client is not None:
                    #     ravdb.update_subgraph(subgraph, status='assigned')
                    # else:
                    ravdb.update_subgraph(subgraph, status='assigned')
                    assigned_sids = []
                    for client in idle_clients:
                        ravdb.update_client(client, reporting='busy', current_subgraph_id=subgraph.subgraph_id,
                                        current_graph_id=subgraph.graph_id)
                        ravdb.create_subgraph_client_mapping(client_id=client.id, graph_id=subgraph.graph_id,
                                                                            subgraph_id=subgraph.subgraph_id)
                        assigned_sids.append(client.sid)
                                        
                        res = requests.get("http://localhost:{}/comms/assigned/?sid={}&subgraph_id={}&graph_id={}&mode=0".format(client.port,client.sid, subgraph.subgraph_id, subgraph.graph_id))
                
                        
                        if res.status_code == 300:
                            ravdb.update_subgraph(subgraph, status='ready')
                            g.logger.debug("\nFailed to assign Subgraph ID: {} Graph ID: {} to Clients: {}".format(subgraph.subgraph_id, subgraph.graph_id, str(assigned_sids)))

                    print("Assigned: {} / {} ----> Clients: {}".format(subgraph.subgraph_id, subgraph.graph_id, str(assigned_sids)))
                

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


def disconnect_client(client):
    g.logger.debug('Disconnecting Client: {}\n'.format(client.cid))
    ravdb.update_client(client, status="disconnected", reporting='ready', disconnected_at=datetime.datetime.utcnow())
    assigned_subgraph = ravdb.get_subgraph(client.current_subgraph_id, client.current_graph_id)
    if assigned_subgraph is not None:
        failure_flag = False
        if assigned_subgraph.retry_attempts > 0:
            if assigned_subgraph.status != "computed" and assigned_subgraph.status != "hold":
                failure_flag = True
                ravdb.update_subgraph(assigned_subgraph, status="failed", has_failed="False", retry_attempts=assigned_subgraph.retry_attempts - 1)
        else:
            if assigned_subgraph.status != "computed" and assigned_subgraph.status != "hold":
                failure_flag = True
                ravdb.update_subgraph(assigned_subgraph, status="failed", has_failed="False")

        graph = ravdb.get_graph(graph_id=assigned_subgraph.graph_id)
        if failure_flag:
            ravdb.update_graph(graph, failed_subgraph="True")
        current_subgraph_id = client.current_subgraph_id
        current_graph_id = client.current_graph_id

        if client.stashed_queue is not None:
            stashed_queue = ast.literal_eval(client.stashed_queue)
            graph_stash_amount = stashed_queue.get(client.affiliated_graph_id,None)
            if graph_stash_amount is not None:
                stashed_queue[client.affiliated_graph_id] += client.staked_amount
            else:
                stashed_queue[client.affiliated_graph_id] = client.staked_amount
        else:
            stashed_queue = {}
            stashed_queue[client.affiliated_graph_id] = client.staked_amount

        # call ravenauth endpoint - /auth/wallet/
        access_token = verify_and_get_access_token()
        response = requests.put(RAVENAUTH_WALLET, {"cid":client.cid,"tokens_to_stash": client.staked_amount}, headers={
            'Authorization': "Bearer " + access_token
        })

        # if response.status_code != 200:
        #     await g.sio.emit('invalid_graph', {"message": "Unable to update wallet tokens:{}".format(response.text)}, namespace='/client', room=client.sid)
        #     return

        ravdb.update_client(client, current_subgraph_id=None, current_graph_id=None, stashed_queue=str(stashed_queue))

        # zip_file_path = FTP_FILES_PATH + '/' + str(client.cid) + '/zip_{}_{}.zip'.format(
        #     current_subgraph_id,
        #     current_graph_id,
        # )

        # if os.path.exists(zip_file_path):
        #     os.remove(zip_file_path)

        if failure_flag:

            local_zip_file_path = FTP_FILES_PATH + '/' + str(client.cid) + '/local_{}_{}.zip'.format(
                current_subgraph_id,
                current_graph_id,
            )

            if os.path.exists(local_zip_file_path):
                os.remove(local_zip_file_path)

        ravdb.update_graph(graph, active_participants = graph.active_participants - 1, proportioned = "False")

    else:
        affiliated_graph = ravdb.get_graph(client.affiliated_graph_id)
        if affiliated_graph is not None:
            if client.stashed_queue is not None:
                stashed_queue = ast.literal_eval(client.stashed_queue)
                graph_stash_amount = stashed_queue.get(client.affiliated_graph_id,None)
                if graph_stash_amount is not None:
                    stashed_queue[client.affiliated_graph_id] += client.staked_amount
                else:
                    stashed_queue[client.affiliated_graph_id] = client.staked_amount
            else:
                stashed_queue = {}
                stashed_queue[client.affiliated_graph_id] = client.staked_amount

            # call ravenauth endpoint - /auth/wallet/
            access_token = verify_and_get_access_token()
            response = requests.put(RAVENAUTH_WALLET, {"cid":client.cid, "tokens_to_stash": client.staked_amount}, headers={
                'Authorization': "Bearer " + access_token
            })
            print('response: ', response.text)

            # if response.status_code != 200:
            #     await g.sio.emit('invalid_graph', {"message": "Unable to update wallet tokens:{}".format(response.text)}, namespace='/client', room=client.sid)
            #     return

            ravdb.update_client(client, current_subgraph_id=None, current_graph_id=None, stashed_queue=str(stashed_queue))

            if affiliated_graph.active_participants > 0:
                ravdb.update_graph(affiliated_graph, active_participants = affiliated_graph.active_participants - 1, proportioned = "False")

def clear_assigned_subgraphs(mapping):
    """
    Clear assigned subgraphs
    """
    # 1. Update subgraph_client_mapping
    ravdb.update_subgraph_client_mapping(mapping.id, status="failed")

    # 2. Update subgraph status
    subgraph = ravdb.get_subgraph(subgraph_id=mapping.subgraph_id, graph_id=mapping.graph_id)
    ravdb.update_subgraph(subgraph, status=SubgraphStatus.READY, optimized="False")

    # 3. Update op statuses
    op_ids = ast.literal_eval(subgraph.op_ids)
    for op_id in op_ids:
        op = ravdb.get_op(op_id)
        if op.status != "computed":
            ravdb.update_op(op, status=OpStatus.PENDING, subgraph_id=subgraph.subgraph_id)

    # 4. Update client status
    client_id = mapping.client_id
    client = ravdb.get_client(client_id)
    ravdb.update_client(client, reporting="broken_connection", current_subgraph_id=None, current_graph_id=None)

if __name__ == '__main__':
    run_scheduler()
