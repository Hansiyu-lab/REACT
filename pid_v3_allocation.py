"""PID-v3-only helpers. Arrays always follow the caller's nodes1 order."""
import warnings
import numpy as np
from ctrl.pid import PID


def blocking_inputs(nodes1, queues, average, tail, weight, previous_counts):
    """Allow padded SLA columns; require metrics only for observed service flows."""
    if not nodes1 or set(nodes1) != set(previous_counts):
        raise ValueError("PID requires measurements for every controlled service")
    weights = np.asarray(weight, dtype=float)
    if weights.shape != (3,) or not np.all(np.isfinite(weights)) or np.any(weights < 0):
        raise ValueError("PID metric weights must be three finite nonnegative values")
    blocking = {}
    for node in nodes1:
        flows = queues[node]
        if not flows:
            raise ValueError(f"No observed flows for {node}")
        totals = []
        for source in (queues, average, tail):
            if node not in source or not set(flows).issubset(source[node]):
                raise ValueError(f"Missing observed-flow measurement for {node}")
            values = np.asarray([source[node][flow] for flow in flows], dtype=float)
            if not np.all(np.isfinite(values)):
                raise ValueError(f"Nonfinite measurement for {node}")

            totals.append(float(np.maximum(values, 0.0).sum()))
        blocking[node] = float(np.dot(weights, totals))
    if not np.all(np.isfinite(list(blocking.values()))):
        raise ValueError("PID blocking overflow")
    return blocking


def allocate_actual_pods(nodes1, candidates, final_total_capacity, blocking):
    """Ratio * actual budget, minimum one, then blocking-weighted remainders."""
    n = len(nodes1)
    capacity = int(final_total_capacity)
    if capacity != final_total_capacity or capacity < n:
        raise ValueError("Actual Pod budget must be an integer >= service count")
    candidates = np.asarray(candidates, dtype=float)
    blocking = np.asarray(blocking, dtype=float)
    if candidates.shape != (n,) or blocking.shape != (n,):
        raise ValueError("Allocation dimension mismatch")
    if (not np.all(np.isfinite(candidates)) or np.any(candidates <= 0)
            or not np.all(np.isfinite(blocking)) or np.any(blocking < 0)):
        raise ValueError("Invalid allocation values")
    ratios = candidates / candidates.sum()
    desired = ratios * capacity

    locked = np.zeros(n, dtype=bool)
    while np.any((desired < 1.0) & ~locked):
        locked |= desired < 1.0
        desired[locked] = 1.0
        active = ~locked
        if not np.any(active):
            break
        desired[active] = candidates[active] / candidates[active].sum() * (capacity - locked.sum())

    close = np.abs(desired - np.rint(desired)) < 1e-10
    desired[close] = np.rint(desired[close])
    counts = np.floor(desired).astype(int)
    remaining = capacity - int(counts.sum())
    if remaining < 0:
        raise ValueError("Allocation exceeded its budget")
    if remaining:
        remainder_weights = blocking / blocking.sum() if blocking.sum() > 0 else ratios
        quotas = remaining * remainder_weights
        extras = np.floor(quotas).astype(int)
        left = remaining - int(extras.sum())

        ranked = sorted(range(n), key=lambda i: (-(quotas[i] - extras[i]), nodes1[i]))
        for i in ranked[:left]:
            extras[i] += 1
        counts += extras
    if np.any(counts < 1) or int(counts.sum()) != capacity:
        raise ValueError("Invalid final Pod allocation")
    return counts, ratios


def control_counts(nodes1, tar_blocking, blocking_middle_array, previous_counts, final_total_capacity, plant_model, state, k1=10, k2=100, b=30, dt=0.1):

    target = np.asarray(tar_blocking, dtype=float)
    measured = np.asarray(blocking_middle_array, dtype=float)
    counts = np.asarray([previous_counts[node] for node in nodes1], dtype=float)
    n = len(nodes1)
    if target.shape != (n,) or measured.shape != (n,):
        raise ValueError("PID blocking dimension mismatch")
    if (not np.all(np.isfinite(target)) or not np.all(np.isfinite(measured))
            or not np.all(np.isfinite(counts)) or np.any(counts < 1)
            or np.any(counts != np.floor(counts))):
        raise ValueError("Invalid PID inputs or previous actual Pod counts")
    if int(final_total_capacity) != final_total_capacity or final_total_capacity < n:
        raise ValueError("Invalid actual capacity")
    if state is None:
        warnings.warn("Pass pid_state={} once per experiment to preserve PID history", RuntimeWarning)
        state = {}
    if 'pid' not in state:
        cfg = PID.getConfig()(dt=dt, dim=n, Kp=0.05, Ki=0.05, Kd=0.01,
                              u_min=-200.0, u_max=200.0)
        state['pid'] = PID(cfg)
        state['nodes'] = list(nodes1)

        state['pid'].last_error = (target - measured).copy()
    pid = state['pid']
    if set(state['nodes']) != set(nodes1):
        raise ValueError("Controlled service set changed during PID experiment")
    if state['nodes'] != nodes1:
        indices = [state['nodes'].index(node) for node in nodes1]

        for name in ('u', 'error', 'last_error', 'error_diff', 'error_sum', 'anti_error',
                     'Kp', 'Ki', 'Kd', 'Kaw', 'Kf', 'u_min', 'u_max', 'ins_max_err'):
            setattr(pid, name, getattr(pid, name)[indices].copy())
        state['nodes'] = list(nodes1)
    gain = dt / (k1 + k2 * dt)
    pid.u_min = np.full(n, -200.0)
    pid.u_max = np.minimum(200.0, (counts - 1.0) / gain)
    old_integral = pid.anti_error.copy()

    signal = pid(target, measured, anti_windup_method=2)

    plant_input = b + k2 * counts - signal
    candidates = np.maximum(1.0, plant_model(counts, plant_input, k1, k2, b, dt))
    actual, ratios = allocate_actual_pods(nodes1, candidates, final_total_capacity, measured)

    error = target - measured
    blocked = ((error < 0) & (actual <= counts)) | ((error > 0) & (actual >= counts))
    blocked &= (pid.anti_error - old_integral) * error > 0
    pid.anti_error[blocked] = old_integral[blocked]
    pid.error_sum = pid.anti_error.copy()
    pid.logger.i[-1] = pid.anti_error.copy()
    state['diagnostics'] = {
        'nodes1': list(nodes1), 'target_blocking_us': target.tolist(),
        'blocking_us': measured.tolist(), 'previous_actual_pods': counts.tolist(),
        'pid_output': signal.tolist(), 'plant_input': plant_input.tolist(),
        'candidate_actual_pods': candidates.tolist(), 'candidate_ratios': ratios.tolist(),
        'final_total_capacity_actual': int(final_total_capacity),
        'actual_pods': dict(zip(nodes1, actual.tolist())),
        'integral': pid.anti_error.tolist(), 'allocation_integral_frozen': blocked.tolist(),
        'model_dt': dt,
    }
    return dict(zip(nodes1, actual.tolist()))


def read_actual_pod_counts(manager, nodes):
    """Read current cloud+edge Deployment desired replicas (not Ready counts)."""
    counts = {}
    for node in nodes:
        counts[node] = sum(
            manager.api_client_appsv1.read_namespaced_deployment(
                f'{node}-{tier}', manager.namespace).spec.replicas or 0
            for tier in ('edge', 'cloud'))
        if counts[node] < 1:
            raise ValueError(f"No desired replicas for controlled service {node}")
    return counts
