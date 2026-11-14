from utils import filter_nan_from_dict
import random

def normalize_probabilities(probabilities):
    total = sum(probabilities)
    normalized_probabilities = [prob / total for prob in probabilities]
    return normalized_probabilities

def sample_from_prob_list(elements, blocking):
    if len(elements) != len(blocking):
        raise ValueError("Number of elements must be equal to the number of probabilities.")
    probabilities = normalize_probabilities(blocking)
    sampled_element = random.choices(elements, weights=probabilities, k=1)[0]
    return sampled_element

def prop_schedule_sla2(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, weight, total_capacity):

    queues_estimation = filter_nan_from_dict(queues_estimation)
    total_queue_in_system, total_ave_delay_vio, total_tail_delay_vio = 0.0, 0.0, 0.0
    queue_per_node, ave_delay_vio_per_node, tail_delay_vio_per_node = {}, {}, {}
    for node in queues_estimation:
        queue_per_node[node] = sum([queue_per_flow for _, queue_per_flow in queues_estimation[node].items()])
        total_queue_in_system += queue_per_node[node]

        ave_delay_vio_per_node[node] = sum([max(0, ave_delay_vio_per_flow) for _, ave_delay_vio_per_flow in ave_delay_vio_estimation[node].items()])
        total_ave_delay_vio += ave_delay_vio_per_node[node]

        tail_delay_vio_per_node[node] = sum([max(0, tail_delay_vio_per_flow) for _, tail_delay_vio_per_flow in tail_delay_vio_estimation[node].items()])
        total_tail_delay_vio += tail_delay_vio_per_node[node]

    blocking_per_node, total_blocking = {}, 0.0
    blocking_per_node_middle, total_blocking_middle = {}, 0.0
    for node in queues_estimation:
        blocking_per_node[node] = weight[0] * queue_per_node[node]/total_queue_in_system if total_queue_in_system > 0 else 0.0
        blocking_per_node[node] += weight[1] * ave_delay_vio_per_node[node]/total_ave_delay_vio if total_ave_delay_vio > 0 else 0.0
        blocking_per_node[node] += weight[2] * tail_delay_vio_per_node[node]/total_tail_delay_vio if total_tail_delay_vio > 0 else 0.0
        total_blocking += blocking_per_node[node]

        blocking_per_node_middle[node] = weight[0] * queue_per_node[node] if total_queue_in_system > 0 else 0.0
        blocking_per_node_middle[node] += weight[1] * ave_delay_vio_per_node[node] if total_ave_delay_vio > 0 else 0.0
        blocking_per_node_middle[node] += weight[2] * tail_delay_vio_per_node[node] if total_tail_delay_vio > 0 else 0.0
        total_blocking_middle += blocking_per_node_middle[node]
    print(f"blocking_per_node_middle:\n{blocking_per_node_middle}")
    print(f"total_blocking_middle:\n{total_blocking_middle}")

    pod_on_node = {}
    left_pods = total_capacity
    for node in queues_estimation:
        pod_on_node[node] = total_capacity*blocking_per_node[node]/total_blocking if total_blocking > 0 else 0.0
        pod_on_node[node] = int(pod_on_node[node])
        left_pods -= pod_on_node[node]

    if total_blocking == 0:
        return pod_on_node

    nodes, blocking = zip(*blocking_per_node.items())
    probs = normalize_probabilities(blocking)

    while left_pods > 0:
        node = sample_from_prob_list(nodes, probs)
        pod_on_node[node] += 1
        left_pods -= 1

    return pod_on_node

import numpy as np

def PlantModel_1(u, max_total_capacity, k=0.1, b=10):

    adjusted_capacity = k * u + b

    adjusted_capacity = max(1,min(round(adjusted_capacity),max_total_capacity))
    print("adjusted_capacity\n",adjusted_capacity)
    return adjusted_capacity

def PlantModel_2(y, u, k1=10, k2=100, b=30, dt=0.1):
    y_1 = (u-b) * dt / (k1+k2*dt) + k1 * y / (k1+k2*dt)
    return y_1

def PlantModel_2_v2(y, u, w1=10, w2=100, dt=0.1):

    numerator = w1 * y + w2 * (y + u) * dt
    denominator = w1 + w2 * dt

    y_next = numerator / denominator
    return y_next

def _select_congestion_eta(delta_blocking, delta_capacity, eta1, eta2):
    values = (delta_blocking, delta_capacity, eta1, eta2)
    if any(not np.isfinite(value) for value in values):
        raise ValueError("Nonfinite congestion compensation input")
    if not 0 < eta1 < eta2:
        raise ValueError("Congestion coefficients must satisfy 0 < eta1 < eta2")

    direction_product = delta_blocking * delta_capacity
    if direction_product == 0:
        return 0.0
    if direction_product < 0:
        return float(eta1)
    return float(eta2)

def _gca_extra_capacity(workload_rate, deta_rate, total_blocking_middle,
                        deta_total_pod, last_total_blocking,
                        max_total_capacity):
    values = (workload_rate, deta_rate, total_blocking_middle, deta_total_pod)
    if any(not np.isfinite(value) for value in values):
        raise ValueError("Nonfinite outer controller input")
    if (not np.isfinite(max_total_capacity) or max_total_capacity < 0 or
            int(max_total_capacity) != max_total_capacity):
        raise ValueError("Invalid extra Pod capacity")
    if last_total_blocking is not None and not np.isfinite(last_total_blocking):
        raise ValueError("Nonfinite previous blocking")

    alpha, beta, bias = 0.01, 0.00047, 0.2
    b_ref = 100000
    eta1, eta2 = 1.3, 1.5

    mu = 0.005

    delta_blocking = (
        (total_blocking_middle - last_total_blocking) / b_ref
        if last_total_blocking is not None else 0.0
    )
    base_capacity = (
        alpha * workload_rate + beta * total_blocking_middle + bias
    )
    term_rate = mu * deta_rate
    direction_product = delta_blocking * deta_total_pod
    selected_eta = _select_congestion_eta(
        delta_blocking, deta_total_pod, eta1, eta2)
    term_blocking = selected_eta * delta_blocking

    adjusted_capacity = base_capacity + term_rate + term_blocking
    extra_capacity = max(
        0, min(round(adjusted_capacity), int(max_total_capacity))
    )
    if total_blocking_middle == 0:
        extra_capacity = 0
    diagnostics = {
        "parameters": {
            "alpha": alpha, "beta": beta, "bias": bias,
            "B_ref": b_ref, "eta1": eta1, "eta2": eta2,
            "mu": mu,
        },
        "delta_blocking": float(delta_blocking),
        "delta_capacity": float(deta_total_pod),
        "direction_product": float(direction_product),
        "selected_eta": float(selected_eta),
        "base_capacity": float(base_capacity),
        "term_rate": float(term_rate),
        "term_blocking": float(term_blocking),
        "adjusted_capacity": float(adjusted_capacity),
        "extra_capacity": int(extra_capacity),
    }
    return int(extra_capacity), diagnostics

def pid_schedule_v3(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, weight, workload_rate,deta_rate, max_total_capacity,last_pod_on_node,deta_total_pod, last_total_blocking, *, pid_state=None):

    from pid_v3_allocation_110 import blocking_inputs, control_counts

    k1 = 150
    k2 = 1500
    b = 30
    dt = 0.1
    node_target_blocking ={
        'frontend': 6000,
        'geo': 300,
        'profile': 1700,
        'rate': 1100,
        'reservation': 4000,
        'search': 3000,
        'recommendation': 250,
        'user': 550
    }

    nodes1 = list(queues_estimation.keys())

    dim = len(nodes1)
    if dim == 0:
        raise ValueError("No PID service measurements")
    if not set(nodes1).issubset(node_target_blocking):
        raise ValueError("PID target missing for an observed service")
    if not np.isfinite(max_total_capacity) or max_total_capacity < 0 or int(max_total_capacity) != max_total_capacity:
        raise ValueError("Invalid extra Pod capacity")
    for value in (workload_rate, deta_rate, deta_total_pod):
        if not np.isfinite(value):
            raise ValueError("Nonfinite outer controller input")
    if last_total_blocking is not None and not np.isfinite(last_total_blocking):
        raise ValueError("Nonfinite previous blocking")

    print(f"================================================================================")
    print(f"进入pid_schedule_v3函数，workload_rate: {workload_rate}, deta_rate: {deta_rate}, deta_total_pod: {deta_total_pod}, last_total_blocking: {last_total_blocking}")

    blocking_per_node_middle = blocking_inputs(
        nodes1, queues_estimation, ave_delay_vio_estimation,
        tail_delay_vio_estimation, weight, last_pod_on_node)
    total_blocking_middle = sum(blocking_per_node_middle.values())

    print(f"total_blocking_middle: {total_blocking_middle}")

    pod_on_node = {}

    extra_capacity, gca_diagnostics = _gca_extra_capacity(
        workload_rate, deta_rate, total_blocking_middle, deta_total_pod,
        last_total_blocking, max_total_capacity)
    print(f"deta_total_blocking: {gca_diagnostics['delta_blocking']}")
    print(f"base_capacity: {gca_diagnostics['base_capacity']}")
    print(f"term_rate: {gca_diagnostics['term_rate']}")
    print(f"term_blocking: {gca_diagnostics['term_blocking']}")
    final_total_capacity = int(extra_capacity) + dim
    print("外层控制的总容量final_total_capacity：", final_total_capacity)
    print(f"================================================================================\n")
    print(f"================================================================================\n")

    print(f"blocking_per_node_middle:\n{blocking_per_node_middle}")

    tar_blocking = np.array([node_target_blocking.get(node, 0) for node in nodes1])
    print("PID 算法目标输入tar_list：", tar_blocking)
    blocking_middle_array = np.array([blocking_per_node_middle.get(node, 0.0) for node in nodes1])
    print("curr_blocking_middle_array:",blocking_middle_array)

    actual_pods = control_counts(
        nodes1, tar_blocking, blocking_middle_array, last_pod_on_node,
        final_total_capacity, PlantModel_2, pid_state, k1, k2, b, dt)

    pod_on_node = {node: actual_pods[node] - 1 for node in nodes1}
    if pid_state is not None:
        pid_state['diagnostics']['blocking_by_service'] = blocking_per_node_middle
        pid_state['diagnostics']['extra_capacity'] = int(extra_capacity)
        pid_state['diagnostics']['extra_pods'] = pod_on_node.copy()
    print("分配剩余pod后pod_on_node (extra):",pod_on_node)

    return pod_on_node, total_blocking_middle

def pid_schedule_v3_wo_AC(
        queues_estimation, ave_delay_vio_estimation,
        tail_delay_vio_estimation, weight, max_total_capacity,
        last_pod_on_node, *, pid_state=None):
    from pid_v3_allocation_110 import blocking_inputs, control_counts

    k1 = 150
    k2 = 1500
    b = 30
    dt = 0.1
    node_target_blocking = {
        "frontend": 6000,
        "geo": 300,
        "profile": 1700,
        "rate": 1100,
        "reservation": 4000,
        "search": 3000,
        "recommendation": 250,
        "user": 550,
    }

    nodes1 = list(queues_estimation.keys())
    dim = len(nodes1)
    if dim == 0:
        raise ValueError("No PID service measurements")
    if not set(nodes1).issubset(node_target_blocking):
        raise ValueError("PID target missing for an observed service")
    if (
        not np.isfinite(max_total_capacity)
        or max_total_capacity < 0
        or int(max_total_capacity) != max_total_capacity
    ):
        raise ValueError("Invalid extra Pod capacity")

    blocking_per_node_middle = blocking_inputs(
        nodes1,
        queues_estimation,
        ave_delay_vio_estimation,
        tail_delay_vio_estimation,
        weight,
        last_pod_on_node,
    )
    total_blocking_middle = sum(blocking_per_node_middle.values())
    final_total_capacity = int(max_total_capacity) + dim

    tar_blocking = np.array(
        [node_target_blocking[node] for node in nodes1], dtype=float
    )
    blocking_middle_array = np.array(
        [blocking_per_node_middle[node] for node in nodes1], dtype=float
    )
    actual_pods = control_counts(
        nodes1,
        tar_blocking,
        blocking_middle_array,
        last_pod_on_node,
        final_total_capacity,
        PlantModel_2,
        pid_state,
        k1,
        k2,
        b,
        dt,
    )
    pod_on_node = {node: actual_pods[node] - 1 for node in nodes1}
    if pid_state is not None:
        pid_state["diagnostics"]["blocking_by_service"] = (
            blocking_per_node_middle
        )
        pid_state["diagnostics"]["extra_capacity"] = int(max_total_capacity)
        pid_state["diagnostics"]["extra_pods"] = pod_on_node.copy()
        pid_state["diagnostics"]["total_blocking"] = float(
            total_blocking_middle
        )
    return pod_on_node

def pid_schedule_v3_AC_with_average(
        queues_estimation, ave_delay_vio_estimation,
        tail_delay_vio_estimation, weight, workload_rate, deta_rate,
        max_total_capacity, deta_total_pod, last_total_blocking,
        last_pod_on_node=None, *, average_weights=None, gca_state=None):
    from pid_v3_allocation_110 import blocking_inputs

    nodes1 = list(queues_estimation.keys())
    if not nodes1:
        raise ValueError("No GCA service measurements")
    previous_counts = last_pod_on_node or dict.fromkeys(nodes1, 1)
    historical_weights = average_weights or {
        "frontend": 1.2,
        "geo": 0.1,
        "profile": 0.4,
        "rate": 0.3,
        "reservation": 0.4,
        "search": 0.3,
        "recommendation": 0.1,
        "user": 0.2,
    }
    if set(historical_weights) != set(nodes1):
        raise ValueError("Average weights do not match controlled services")

    print("=" * 80)
    print(
        "进入pid_schedule_v3_AC_with_average函数，workload_rate: "
        f"{workload_rate}, deta_rate: {deta_rate}, "
        f"deta_total_pod: {deta_total_pod}, "
        f"last_total_blocking: {last_total_blocking}"
    )

    blocking_per_node_middle = blocking_inputs(
        nodes1, queues_estimation, ave_delay_vio_estimation,
        tail_delay_vio_estimation, weight, previous_counts)
    total_blocking_middle = sum(blocking_per_node_middle.values())
    print(f"total_blocking_middle: {total_blocking_middle}")

    extra_capacity, diagnostics = _gca_extra_capacity(
        workload_rate, deta_rate, total_blocking_middle, deta_total_pod,
        last_total_blocking, max_total_capacity)
    diagnostics.update({
        "blocking_by_service": blocking_per_node_middle.copy(),
        "previous_actual_pods": dict(previous_counts),
        "actual_total_capacity": int(extra_capacity) + len(nodes1),
    })
    print(f"deta_total_blocking: {diagnostics['delta_blocking']}")
    print(f"base_capacity: {diagnostics['base_capacity']}")
    print(f"term_rate: {diagnostics['term_rate']}")
    print(f"term_blocking: {diagnostics['term_blocking']}")
    print("外层控制的额外容量extra_capacity：", extra_capacity)
    print("=" * 80 + "\n")

    static_pod_on_node = average_static_allocation(
        historical_weights, extra_capacity)
    diagnostics["extra_pods"] = static_pod_on_node.copy()
    if gca_state is not None:
        gca_state["diagnostics"] = diagnostics
    return static_pod_on_node, total_blocking_middle

def pid_schedule_v3_AC_with_propsla(
        queues_estimation, ave_delay_vio_estimation,
        tail_delay_vio_estimation, gca_weight, workload_rate, deta_rate,
        max_total_capacity, deta_total_pod, last_total_blocking,
        last_pod_on_node=None, *, propsla_weight=None, gca_state=None):
    from pid_v3_allocation_110 import blocking_inputs

    nodes1 = list(queues_estimation.keys())
    if not nodes1:
        raise ValueError("No GCA service measurements")
    previous_counts = last_pod_on_node or dict.fromkeys(nodes1, 1)
    inner_weight = propsla_weight if propsla_weight is not None else gca_weight

    print("=" * 80)
    print(
        "进入pid_schedule_v3_AC_with_propsla函数，workload_rate: "
        f"{workload_rate}, deta_rate: {deta_rate}, "
        f"deta_total_pod: {deta_total_pod}, "
        f"last_total_blocking: {last_total_blocking}"
    )

    blocking_per_node_middle = blocking_inputs(
        nodes1, queues_estimation, ave_delay_vio_estimation,
        tail_delay_vio_estimation, gca_weight, previous_counts)
    total_blocking_middle = sum(blocking_per_node_middle.values())
    print(f"total_blocking_middle: {total_blocking_middle}")

    extra_capacity, diagnostics = _gca_extra_capacity(
        workload_rate, deta_rate, total_blocking_middle, deta_total_pod,
        last_total_blocking, max_total_capacity)
    diagnostics.update({
        "blocking_by_service": blocking_per_node_middle.copy(),
        "previous_actual_pods": dict(previous_counts),
        "actual_total_capacity": int(extra_capacity) + len(nodes1),
        "propsla_weight": list(inner_weight),
    })
    print(f"deta_total_blocking: {diagnostics['delta_blocking']}")
    print(f"base_capacity: {diagnostics['base_capacity']}")
    print(f"term_rate: {diagnostics['term_rate']}")
    print(f"term_blocking: {diagnostics['term_blocking']}")
    print("外层控制的额外容量extra_capacity：", extra_capacity)
    print("=" * 80 + "\n")

    pod_on_node = prop_schedule_sla2(
        queues_estimation, ave_delay_vio_estimation,
        tail_delay_vio_estimation, inner_weight, extra_capacity)
    if set(pod_on_node) != set(nodes1):
        raise ValueError("PropSLA allocation does not cover all services")
    allocated_extra = sum(pod_on_node.values())
    if allocated_extra != extra_capacity:
        raise ValueError(
            "PropSLA did not allocate the complete GCA extra capacity")
    diagnostics["gca_extra_capacity"] = int(extra_capacity)
    diagnostics["allocated_extra_capacity"] = int(allocated_extra)
    diagnostics["extra_pods"] = pod_on_node.copy()
    if gca_state is not None:
        gca_state["diagnostics"] = diagnostics
    return pod_on_node, total_blocking_middle

def average_static_allocation(weights,total_capacity):
    pod_allocation = {}
    total_weight = sum(weights.values())

    remaining_capacity = total_capacity

    for svc, w in weights.items():

        count = int(total_capacity * (w / total_weight))
        pod_allocation[svc] = count
        remaining_capacity -= count

    if remaining_capacity > 0:

        max_svc = max(weights, key=weights.get)
        pod_allocation[max_svc] += remaining_capacity

    return pod_allocation

