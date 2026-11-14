import argparse
import datetime
import time
import pandas as pd
import logging
import threading
import os
from jaegerCollector import JaegerCollector
from k8sManager import K8sManager
from workloadGenerator import WorkloadGenerator
from algorithm import (
    average_static_allocation,
    pid_schedule_v3,
    pid_schedule_v3_AC_with_average,
    pid_schedule_v3_AC_with_propsla,
    pid_schedule_v3_wo_AC,
    prop_schedule_sla2,
)
from utils import (
    calculate_ave_latency_vio,
    calculate_tail,
    calculate_tail_latency_vio,
    filter_nan_from_dict,
    get_trace_deployment_table,
    init_env,
    init_pod_scaling,
    init_pod_scaling_max,
    load_resources_config,
    prepare_dynamic_workload,
    save_blocking_middle_data,
    save_delay_result,
    save_dict_to_json_v2,
    transform_queue_estimation,
)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

file_directory = os.path.dirname(os.path.abspath(__file__))
result_path = os.path.join(file_directory, "output_performance_data") + os.sep
pod_resources_path = os.path.join(
    file_directory, "input", "pod_resource", "resources_well_v2_HPA.json")
wrk_pattern_file = os.path.join(file_directory, "input", "pattern_4.csv")
sla_file_path = os.path.join(file_directory, "input", "sla", "ave_sla.csv")
tail_sla_file_path = os.path.join(
    file_directory, "input", "sla", "tail_sla.csv")

TASKS = ["HTTP GET /hotels", "HTTP GET /recommendations", "HTTP POST /reservation", "HTTP POST /user"]
LAST_POD_ON_NODE = {'frontend':8, 'geo': 3, 'profile': 3, 'rate': 3, 'reservation': 3, 'search': 6,'recommendation': 3,'user': 3}
TOTAL_EPOCHS = 25 
CONTROL_INTERVAL_SECONDS = 90
CONTROL_DYNAMIC_SECONDS = "250s"

TOTAL_POD_CAPACITY = 4*8-8 
jaeger_endpoint = "31363"
wrk_endpoint = "30333"
generate_workload_duration = "45m"
propsla_weight = [0.6, 0.2, 0.2]
pid_weight = [0.6, 0.2, 0.2] 

def collect_delay_data(
        collector: JaegerCollector,
        control_interval=CONTROL_INTERVAL_SECONDS):
    queues_estimation_raw = dict()
    all_trace_latency = dict()
    tail_estimation_raw = dict()
    delay_results = {}
    collect_end_time = time.time()
    for task in TASKS:
        collector.clear()
        raw_traces = collector.collect(
            end_time=collect_end_time,
            duration=control_interval,
            limit=800,
            service="frontend",
            task_type=task,
        )
        if not raw_traces: continue
        
        merged_traces = collector.process_trace_data()
        if merged_traces.empty: continue
        trace_deployment_table = get_trace_deployment_table(merged_df=merged_traces)
        task_name = task.replace("/", "")
        queues_estimation_raw[task_name] = trace_deployment_table.mean(axis=0, numeric_only=True).to_dict()
        all_trace_latency[task_name] = collector.get_all_latency()
        tail_estimation_raw[task_name] = calculate_tail(trace_deployment_table)
        avg_lat, normal_lat, tail_lat, sla_vio_rate = collector.calculate_average_latency(task_name=task_name)
    
        if task_name not in delay_results:
            delay_results[task_name] = {"average":[], "normal":[], "tail":[], "sla":[]}

        delay_results[task_name]["average"].append(avg_lat)
        delay_results[task_name]["normal"].append(normal_lat)
        delay_results[task_name]["tail"].append(tail_lat)
        
        delay_results[task_name]["sla"].append(sla_vio_rate)
    save_dict_to_json_v2(all_trace_latency, all_trace_latency_path) # Save

    if not queues_estimation_raw:
        logging.warning("No data collected for algorithm input. Skipping scaling for this epoch.")
        return delay_results,{}, {}, {}

    queues_estimation = transform_queue_estimation(queues_estimation_raw)

    ave_delay_vio = calculate_ave_latency_vio(pd.DataFrame(queues_estimation).T.fillna("/"),sla_file_path)
    ave_delay_vio_estimation = filter_nan_from_dict(transform_queue_estimation(ave_delay_vio))
    tail_delay_vio = calculate_tail_latency_vio(pd.DataFrame(tail_estimation_raw).fillna("/"),tail_sla_file_path)
    tail_delay_vio_estimation = filter_nan_from_dict(transform_queue_estimation(tail_delay_vio))
    
    return delay_results,queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation

def cul_delay_data_per_node(queues_estimation):
    queues_estimation = filter_nan_from_dict(queues_estimation)
    total_queue_in_system = 0.0
    queue_per_node = {}
    for node in queues_estimation:
        queue_per_node[node] = sum([queue_per_flow for _, queue_per_flow in queues_estimation[node].items()])
        total_queue_in_system += queue_per_node[node]
    return queue_per_node

def cul_and_save_blocking(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, weight, file_path):
    queues_estimation = filter_nan_from_dict(queues_estimation)
    total_queue_in_system, total_ave_delay_vio, total_tail_delay_vio = 0.0, 0.0, 0.0
    queue_per_node, ave_delay_vio_per_node, tail_delay_vio_per_node = {}, {}, {}
    for node in queues_estimation:
        queue_per_node[node] = sum([queue_per_flow for _, queue_per_flow in queues_estimation[node].items()])
        total_queue_in_system += queue_per_node[node]

        ave_delay_vio_per_node[node] = sum([ave_delay_vio_per_flow for _, ave_delay_vio_per_flow in ave_delay_vio_estimation[node].items()])
        total_ave_delay_vio += ave_delay_vio_per_node[node]

        tail_delay_vio_per_node[node] = sum([tail_delay_vio_per_flow for _, tail_delay_vio_per_flow in tail_delay_vio_estimation[node].items()])
        total_tail_delay_vio += tail_delay_vio_per_node[node]
    
    blocking_per_node_middle, total_blocking_middle = {}, 0.0
    for node in queues_estimation:
        blocking_per_node_middle[node] = weight[0] * queue_per_node[node] if total_queue_in_system > 0 else 0.0
        blocking_per_node_middle[node] += weight[1] * ave_delay_vio_per_node[node] if total_ave_delay_vio > 0 else 0.0
        blocking_per_node_middle[node] += weight[2] * tail_delay_vio_per_node[node] if total_tail_delay_vio > 0 else 0.0
        total_blocking_middle += blocking_per_node_middle[node]

    save_blocking_middle_data(blocking_per_node_middle, total_blocking_middle, file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Microservice Autoscaling Algorithm Test Framework.")
    parser.add_argument(
        "test_name", 
        choices=["propsla","pid","average","k8shpa","pid_wo_AC","AC_with_average","AC_with_propsla"],
        help="The name of the test to run."
    )
    parser.add_argument(
        "--rate",
        type=int,
        help="The initial workload rate (target QPS) for the test."
    )
    parser.add_argument(
        "--pod",
        type=int,
        help="The initial workload rate (target QPS) for the test."
    )
    parser.add_argument(
        "--d", 
        action="store_true", 
        help="运行 'pattern_4.csv' 中的动态负载，而不是静态负载"
    )
    args = parser.parse_args()
    TOTAL_POD_CAPACITY = args.pod *8-8  if args.pod else TOTAL_POD_CAPACITY
    
    
    logging.info("Initializing environment...")
    initial_rate = args.rate if args.rate else 2000
    resources = load_resources_config(pod_resources_path)
    k8s_manager = K8sManager("hotel")
    collector = JaegerCollector(sla_file_path, jaeger_endpoint)
    delay_results_all = {}
    
    now = datetime.datetime.now()
    timestamp = now.strftime("%m%d_%H%M")  
    
    if not args.d:
        pod_sum_filename = f"{result_path}{args.test_name}/static/pod_sum_{timestamp}_{args.rate}.json"
        pod_distribution_filename = f"{result_path}{args.test_name}/static/pod_distribution_{timestamp}_{args.rate}.json"
        blocking_file_path =   f"{result_path}{args.test_name}/static/blocking_middle_{timestamp}_{args.rate}.json"
        delay_result_filename = f"{result_path}{args.test_name}/static/delay_{timestamp}_{args.rate}.json"
        all_trace_latency_path = f"{result_path}{args.test_name}/static/all_trace_latency_{timestamp}_{args.rate}.json" 
    else:
        pod_sum_filename = f"{result_path}{args.test_name}/dynamic/pod_sum_dynamic_{timestamp}.json"
        pod_distribution_filename = f"{result_path}{args.test_name}/dynamic/pod_distribution_dynamic_{timestamp}.json"
        blocking_file_path =   f"{result_path}{args.test_name}/dynamic/blocking_middle_dynamic_{timestamp}.json"
        delay_result_filename = f"{result_path}{args.test_name}/dynamic/delay_dynamic_{timestamp}.json"
        all_trace_latency_path = f"{result_path}{args.test_name}/dynamic/all_trace_latency_dynamic_{timestamp}.json" 
    
    
    init_env(k8s_manager,resources) 
    init_pod_scaling_max(k8s_manager, args.pod if args.pod else 2) 
    
    workloadGenerator = WorkloadGenerator(endpoint=wrk_endpoint)
    if args.d:
        logging.info("loading dynamic workload pattern from file: " + "./input/pattern_4.csv")
        dynamic_rate = prepare_dynamic_workload("./input/pattern_4.csv")
        workload_rate = dynamic_rate[0]  
    else:
        logging.info("Starting stationary workload generator...")
        workload_rate = initial_rate
        station_workload_thread = threading.Thread(target=workloadGenerator.generate_stationary, args=(generate_workload_duration,initial_rate), daemon=True)
        station_workload_thread.start()


    if args.test_name not in ["propsla","pid","average","k8shpa","pid_wo_AC","AC_with_average","AC_with_propsla"]:
        logging.error("Invalid test name provided. Exiting.")
        exit(1)
    elif args.test_name == "propsla":
        logging.info("========== Starting Test: "+args.test_name+" ==========")
        for epoch in range(TOTAL_EPOCHS):
            logging.info(f"--- Running PropSLA Epoch {epoch + 1}/{TOTAL_EPOCHS} ---")
            
            if args.d:
                workload_rate = dynamic_rate[epoch]
                logging.info(f"Starting dynamic workload generator with rate: {dynamic_rate[epoch]} QPS...")
                dynamic_workload_thread = threading.Thread(target=workloadGenerator.generate_stationary, args=(CONTROL_DYNAMIC_SECONDS,workload_rate), daemon=True)
                dynamic_workload_thread.start()
            
            time.sleep(CONTROL_INTERVAL_SECONDS)
            
            delay_result,queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation = collect_delay_data(
                collector, CONTROL_INTERVAL_SECONDS)
            
            save_delay_result(delay_results_all, delay_result, delay_result_filename)
            cul_and_save_blocking(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, pid_weight, blocking_file_path)
            
            pod_on_node = prop_schedule_sla2(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, propsla_weight,TOTAL_POD_CAPACITY)
            logging.info(f"Algorithm output: \n{pd.Series(pod_on_node)}")
            
            total_pods = sum(pod_on_node.values())+8
            save_dict_to_json_v2(total_pods, pod_sum_filename, epoch + 1)
            save_dict_to_json_v2(pod_on_node, pod_distribution_filename, epoch + 1)
            
            for deployment_name in pod_on_node:
                pod_on_node[deployment_name] += 1 
            k8s_manager.edge_prior_scale_deployment(pod_on_node)
            
            if args.d:
                dynamic_workload_thread.join()
        
        logging.info("Experiment finished. Terminating workload generator...")

        workloadGenerator.stop_workload() 
        if not args.d:
            station_workload_thread.join(timeout=10)
        init_pod_scaling(k8s_manager,args.pod if args.pod else 1, for_hpa=False)
        logging.info("Resetting environment to initial state.")
        logging.info("Done.")
        
    elif args.test_name == "pid":
        logging.info("========== Starting Test: "+args.test_name+" ==========")
        from pid_v3_allocation_110 import read_actual_pod_counts

        pid_state = {}
        pid_diagnostics_filename = pod_distribution_filename.replace('.json', '_pid_control.json')
        # 初始化变量
        last_workload_rate = workload_rate #
        last_total_blocking = None
        init_pod_on_node = read_actual_pod_counts(k8s_manager, LAST_POD_ON_NODE)
        last_pod_on_node = init_pod_on_node.copy()
        init_total_pod = sum(init_pod_on_node.values()) # 32
        
        prev_1_total = init_total_pod #上一轮 t-1
        prev_2_total = init_total_pod #上上轮 t-2
        
        for epoch in range(TOTAL_EPOCHS):
            logging.info(f"--- Running PID Epoch {epoch + 1}/{TOTAL_EPOCHS} ---")
            # 动态负载
            if args.d:
                workload_rate = dynamic_rate[epoch]
                logging.info(f"Starting dynamic workload generator with rate: {dynamic_rate[epoch]} QPS...")
                dynamic_workload_thread = threading.Thread(target=workloadGenerator.generate_stationary, args=(CONTROL_DYNAMIC_SECONDS,workload_rate), daemon=True)
                dynamic_workload_thread.start()
                # time.sleep(15) # 确保负载生成器启动完成 
            
            time.sleep(CONTROL_INTERVAL_SECONDS)
            
            # 1. 收集、处理数据，算法输入
            delay_result,queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation = collect_delay_data(collector, CONTROL_INTERVAL_SECONDS)
            save_delay_result(delay_results_all, delay_result, delay_result_filename)
            
            # 2. 定义变量,保存workload_rate和total_blocking_middle的历史记录，用于趋势分析 111
            # pod的变化量
            deta_pod = prev_1_total - prev_2_total
            # 负载的变化量deta_rate
            deta_rate = workload_rate - last_workload_rate 
            
            # 3. 运行算法并执行扩缩容 111
            try:
                # Use current desired replicas, not the hardcoded initial vector.
                last_pod_on_node = read_actual_pod_counts(k8s_manager, LAST_POD_ON_NODE)
                pod_on_node, current_total_blocking = pid_schedule_v3(
                    queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation,
                    pid_weight, workload_rate, deta_rate, TOTAL_POD_CAPACITY,
                    last_pod_on_node, deta_pod, last_total_blocking, pid_state=pid_state)
            except ValueError as exc:
                logging.warning("Skipping invalid PID epoch %s: %s", epoch + 1, exc)
                save_dict_to_json_v2({'status': 'skipped', 'reason': str(exc)},
                                     pid_diagnostics_filename, epoch + 1)
                if args.d:
                    dynamic_workload_thread.join()
                continue
            save_blocking_middle_data(pid_state['diagnostics']['blocking_by_service'],
                                      current_total_blocking, blocking_file_path)
            # pod_on_node, current_total_blocking = pid_schedule_v3_without_external_control(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, 
            #                                                       pid_weight, TOTAL_POD_CAPACITY, last_pod_on_node)
            
            # 更新负载 111
            last_workload_rate = workload_rate
            last_total_blocking = current_total_blocking
            
            logging.info(f"Algorithm output: \n{pd.Series(pod_on_node)}")
            
            # Return format remains extra counts; add baseline exactly once.
            extra_pods = pod_on_node.copy()
            pod_on_node = {node: count + 1 for node, count in extra_pods.items()}
            total_pods = sum(pod_on_node.values())
            diagnostics = dict(pid_state['diagnostics'], status='planned')
            save_dict_to_json_v2(diagnostics, pid_diagnostics_filename, epoch + 1)
            
            # 更新实际的pod分配
            last_pod_on_node = pod_on_node
            current_total_pod = sum(pod_on_node.values())
            # 更新实际pod总数的历史记录 111
            prev_2_total = prev_1_total # 旧的 t-1 变成新的 t-2
            prev_1_total = current_total_pod # 当前的变成新的 t-1
            
            # 4. 执行部署
            k8s_manager.edge_prior_scale_deployment(pod_on_node)
            # These remain desired-count records, not Ready-Pod telemetry.
            observed_counts = read_actual_pod_counts(k8s_manager, LAST_POD_ON_NODE)
            diagnostics.update(status='applied' if observed_counts == pod_on_node else 'mismatch',
                               observed_desired_pods=observed_counts)
            save_dict_to_json_v2(diagnostics, pid_diagnostics_filename, epoch + 1)
            if observed_counts != pod_on_node:
                raise RuntimeError("PID desired replicas differ from the applied allocation")
            save_dict_to_json_v2(total_pods, pod_sum_filename, epoch + 1)
            save_dict_to_json_v2(extra_pods, pod_distribution_filename, epoch + 1)
            
            if args.d:
                dynamic_workload_thread.join()
            
        logging.info("Experiment finished. Terminating workload generator...")
        # 5. 实验结束后，终止后台负载，恢复平均pod设置
        workloadGenerator.stop_workload() 
        if not args.d:
            station_workload_thread.join(timeout=10)
        init_pod_scaling(k8s_manager,args.pod if args.pod else 1,for_hpa=False)
        logging.info("Resetting environment to initial state.")
        logging.info("Done.")
        
    elif args.test_name == "average":
        logging.info("========== Starting Test: "+args.test_name+" ==========")
        historical_weights = {
            "frontend": 1.2,
            "geo": 0.1,
            "profile": 0.4,
            "rate": 0.3,
            "search": 0.3,       
            "reservation": 0.4,  
            "recommendation": 0.1,  
            "user": 0.2
        }
        
        static_pod_on_node = average_static_allocation(historical_weights, TOTAL_POD_CAPACITY)
        
        logging.info(f"--- Static Allocation Strategy Determined ---")
        logging.info(f"Fixed Plan: \n{pd.Series(static_pod_on_node)}")
        logging.info("===============================================")
        
        for epoch in range(TOTAL_EPOCHS):
            logging.info(f"--- Running Static-AVG Epoch {epoch + 1}/{TOTAL_EPOCHS} ---")
            if args.d:
                workload_rate = dynamic_rate[epoch]
                logging.info(f"Starting dynamic workload generator with rate: {dynamic_rate[epoch]} QPS...")
                dynamic_workload_thread = threading.Thread(target=workloadGenerator.generate_stationary, args=(CONTROL_DYNAMIC_SECONDS,workload_rate), daemon=True)
                dynamic_workload_thread.start()
    
            time.sleep(CONTROL_INTERVAL_SECONDS)
            
            delay_result, queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation = collect_delay_data(
                collector, CONTROL_INTERVAL_SECONDS)
            save_delay_result(delay_results_all, delay_result, delay_result_filename)
            cul_and_save_blocking(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, pid_weight, blocking_file_path)
            
            pod_on_node = static_pod_on_node.copy()
            
            logging.info(f"Static Algorithm output: \n{pd.Series(pod_on_node)}")
            
            total_pods = sum(pod_on_node.values()) + 8 
            save_dict_to_json_v2(total_pods, pod_sum_filename, epoch + 1)
            save_dict_to_json_v2(pod_on_node, pod_distribution_filename, epoch + 1)

            for deployment_name in pod_on_node:
                pod_on_node[deployment_name] += 1
            k8s_manager.edge_prior_scale_deployment(pod_on_node)

            if args.d:
                dynamic_workload_thread.join()
        
        logging.info("Experiment finished. Terminating workload generator...")

        workloadGenerator.stop_workload() 
        if not args.d:
            station_workload_thread.join(timeout=10)
        init_pod_scaling(k8s_manager,args.pod if args.pod else 1,for_hpa=False)
        logging.info("Resetting environment to initial state.")
        logging.info("Done.")
        
    elif args.test_name == "k8shpa":
        logging.info("========== Starting Test: "+args.test_name+" (Native K8s HPA) ==========")

        k8s_manager.delete_all_hpa()
        time.sleep(2)
        k8s_manager.apply_hpa_dynamic()
        
        logging.info("====[Init] HPA Rules Applied. Waiting 30s for Metrics Server sync...")
        time.sleep(30)
        
        for epoch in range(TOTAL_EPOCHS):
            logging.info(f"--- Running HPA Epoch {epoch + 1}/{TOTAL_EPOCHS} ---")
            # 动态负载
            if args.d:
                workload_rate = dynamic_rate[epoch]
                logging.info(f"Starting dynamic workload generator with rate: {dynamic_rate[epoch]} QPS...")
                dynamic_workload_thread = threading.Thread(target=workloadGenerator.generate_stationary, args=(CONTROL_DYNAMIC_SECONDS,workload_rate), daemon=True)
                dynamic_workload_thread.start()
                
            time.sleep(CONTROL_INTERVAL_SECONDS)

            delay_result, queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation = collect_delay_data(
                collector, CONTROL_INTERVAL_SECONDS)
            save_delay_result(delay_results_all, delay_result, delay_result_filename)
            cul_and_save_blocking(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, pid_weight, blocking_file_path)

            pod_on_node = k8s_manager.get_current_replicas_map()
            
            logging.info(f"Observed HPA State: \n{pd.Series(pod_on_node)}")

            total_pods = sum(pod_on_node.values())
            save_dict_to_json_v2(total_pods, pod_sum_filename, epoch + 1)
            save_dict_to_json_v2(pod_on_node, pod_distribution_filename, epoch + 1)

            if args.d:
                dynamic_workload_thread.join()
        
        logging.info("Experiment finished. Terminating workload generator...")

        workloadGenerator.stop_workload() 
        if not args.d:
            station_workload_thread.join(timeout=10)
        
        k8s_manager.delete_all_hpa()
        time.sleep(5) 
        init_pod_scaling(k8s_manager,args.pod if args.pod else 1, for_hpa=True)
        
        logging.info("Resetting environment to initial state.")
        logging.info("Done.")
    
    elif args.test_name == "pid_wo_AC": 
        logging.info("========== Starting Test: "+args.test_name+" ==========")
        from pid_v3_allocation_110 import read_actual_pod_counts

        pid_wo_ac_state = {}
        last_pod_on_node = read_actual_pod_counts(k8s_manager, LAST_POD_ON_NODE)
        
        for epoch in range(TOTAL_EPOCHS):
            logging.info(f"--- Running PID_wo_AC Epoch {epoch + 1}/{TOTAL_EPOCHS} ---")
            # 动态负载
            if args.d:
                workload_rate = dynamic_rate[epoch]
                logging.info(f"Starting dynamic workload generator with rate: {dynamic_rate[epoch]} QPS...")
                dynamic_workload_thread = threading.Thread(target=workloadGenerator.generate_stationary, args=(CONTROL_DYNAMIC_SECONDS,workload_rate), daemon=True)
                dynamic_workload_thread.start()

            
            time.sleep(CONTROL_INTERVAL_SECONDS)
            
            delay_result,queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation = collect_delay_data(collector, CONTROL_INTERVAL_SECONDS)
            
            save_delay_result(delay_results_all, delay_result, delay_result_filename)
            cul_and_save_blocking(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, pid_weight, blocking_file_path)

            try:
                last_pod_on_node = read_actual_pod_counts(
                    k8s_manager, LAST_POD_ON_NODE)
                extra_pods = pid_schedule_v3_wo_AC(
                    queues_estimation,
                    ave_delay_vio_estimation,
                    tail_delay_vio_estimation,
                    pid_weight,
                    TOTAL_POD_CAPACITY,
                    last_pod_on_node,
                    pid_state=pid_wo_ac_state,
                )
            except ValueError as exc:
                logging.warning(
                    "Skipping invalid PID_wo_AC epoch %s: %s",
                    epoch + 1,
                    exc,
                )
                if args.d:
                    dynamic_workload_thread.join()
                continue
            logging.info(f"Algorithm output: \n{pd.Series(extra_pods)}")

            pod_on_node = {
                node: count + 1 for node, count in extra_pods.items()
            }
            total_pods = sum(pod_on_node.values())
            save_dict_to_json_v2(total_pods, pod_sum_filename, epoch + 1)
            save_dict_to_json_v2(extra_pods, pod_distribution_filename, epoch + 1)

            k8s_manager.edge_prior_scale_deployment(pod_on_node)

            if args.d:
                dynamic_workload_thread.join()
        
        logging.info("Experiment finished. Terminating workload generator...")

        workloadGenerator.stop_workload() 
        if not args.d:
            station_workload_thread.join(timeout=10)
        init_pod_scaling(k8s_manager,args.pod if args.pod else 1, for_hpa=False)
        logging.info("Resetting environment to initial state.")
        logging.info("Done.")
        
    elif args.test_name == "AC_with_average":
        logging.info("========== Starting Test: "+args.test_name+" ==========")

        last_workload_rate = workload_rate #
        last_total_blocking = None
        init_pod_on_node = LAST_POD_ON_NODE.copy()
        last_pod_on_node = LAST_POD_ON_NODE.copy()
        init_total_pod = sum(init_pod_on_node.values()) 
        
        prev_1_total = init_total_pod 
        prev_2_total = init_total_pod 
                
        for epoch in range(TOTAL_EPOCHS):
            logging.info(f"--- Running AC_with_Average Epoch {epoch + 1}/{TOTAL_EPOCHS} ---")
            if args.d:
                workload_rate = dynamic_rate[epoch]
                logging.info(f"Starting dynamic workload generator with rate: {dynamic_rate[epoch]} QPS...")
                dynamic_workload_thread = threading.Thread(target=workloadGenerator.generate_stationary, args=(CONTROL_DYNAMIC_SECONDS,workload_rate), daemon=True)
                dynamic_workload_thread.start()
    
            time.sleep(CONTROL_INTERVAL_SECONDS)
            
            delay_result, queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation = collect_delay_data(collector, CONTROL_INTERVAL_SECONDS)
            save_delay_result(delay_results_all, delay_result, delay_result_filename)
            cul_and_save_blocking(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, pid_weight, blocking_file_path)

            deta_pod = prev_1_total - prev_2_total
            deta_rate = workload_rate - last_workload_rate 

            pod_on_node, current_total_blocking = pid_schedule_v3_AC_with_average(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, 
                                                                  pid_weight, workload_rate, deta_rate, TOTAL_POD_CAPACITY,
                                                                  deta_pod, last_total_blocking)

            last_workload_rate = workload_rate
            last_total_blocking = current_total_blocking
            
            logging.info(f"Static Algorithm output: \n{pd.Series(pod_on_node)}")

            total_pods = sum(pod_on_node.values()) + 8 
            save_dict_to_json_v2(total_pods, pod_sum_filename, epoch + 1)
            save_dict_to_json_v2(pod_on_node, pod_distribution_filename, epoch + 1)

            for deployment_name in pod_on_node:
                pod_on_node[deployment_name] += 1

            last_pod_on_node = pod_on_node
            current_total_pod = sum(pod_on_node.values())

            prev_2_total = prev_1_total 
            prev_1_total = current_total_pod 
            
            k8s_manager.edge_prior_scale_deployment(pod_on_node)

            if args.d:
                dynamic_workload_thread.join()
        
        logging.info("Experiment finished. Terminating workload generator...")

        workloadGenerator.stop_workload() 
        if not args.d:
            station_workload_thread.join(timeout=10)
        init_pod_scaling(k8s_manager,args.pod if args.pod else 1,for_hpa=False)
        logging.info("Resetting environment to initial state.")
        logging.info("Done.")
        
    elif args.test_name == "AC_with_propsla":
        logging.info("========== Starting Test: "+args.test_name+" ==========")

        last_workload_rate = workload_rate #
        last_total_blocking = None
        init_pod_on_node = LAST_POD_ON_NODE.copy()
        last_pod_on_node = LAST_POD_ON_NODE.copy()
        init_total_pod = sum(init_pod_on_node.values()) 
        
        prev_1_total = init_total_pod
        prev_2_total = init_total_pod 
                
        for epoch in range(TOTAL_EPOCHS):
            logging.info(f"--- Running AC_with_propsla Epoch {epoch + 1}/{TOTAL_EPOCHS} ---")
            if args.d:
                workload_rate = dynamic_rate[epoch]
                logging.info(f"Starting dynamic workload generator with rate: {dynamic_rate[epoch]} QPS...")
                dynamic_workload_thread = threading.Thread(target=workloadGenerator.generate_stationary, args=(CONTROL_DYNAMIC_SECONDS,workload_rate), daemon=True)
                dynamic_workload_thread.start()
    
            time.sleep(CONTROL_INTERVAL_SECONDS)

            delay_result, queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation = collect_delay_data(collector, CONTROL_INTERVAL_SECONDS)
            save_delay_result(delay_results_all, delay_result, delay_result_filename)
            cul_and_save_blocking(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, pid_weight, blocking_file_path)

            deta_pod = prev_1_total - prev_2_total
            deta_rate = workload_rate - last_workload_rate 
            pod_on_node, current_total_blocking = pid_schedule_v3_AC_with_propsla(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, 
                                                                  pid_weight, workload_rate, deta_rate, TOTAL_POD_CAPACITY,
                                                                  deta_pod, last_total_blocking)

            last_workload_rate = workload_rate
            last_total_blocking = current_total_blocking
            
            logging.info(f"Static Algorithm output: \n{pd.Series(pod_on_node)}")

            total_pods = sum(pod_on_node.values()) + 8 
            save_dict_to_json_v2(total_pods, pod_sum_filename, epoch + 1)
            save_dict_to_json_v2(pod_on_node, pod_distribution_filename, epoch + 1)

            for deployment_name in pod_on_node:
                pod_on_node[deployment_name] += 1

            last_pod_on_node = pod_on_node
            current_total_pod = sum(pod_on_node.values())
            prev_2_total = prev_1_total 
            prev_1_total = current_total_pod 
            
            k8s_manager.edge_prior_scale_deployment(pod_on_node)

            if args.d:
                dynamic_workload_thread.join()
        
        logging.info("Experiment finished. Terminating workload generator...")

        workloadGenerator.stop_workload() 
        if not args.d:
            station_workload_thread.join(timeout=10)
        init_pod_scaling(k8s_manager,args.pod if args.pod else 1,for_hpa=False)
        logging.info("Resetting environment to initial state.")
        logging.info("Done.")
        
        
