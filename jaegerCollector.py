import requests
import json
import numpy as np
import pandas as pd
import re
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None
import logging

class JaegerCollector:

    def __init__(self, sla_file_path, endpoint: str="16686"):
        self.endpoint = f"http://127.0.0.1:{endpoint}/api/traces"
        self.traces = None

        try:

            self.sla_df = pd.read_csv(sla_file_path, index_col=0)
            logging.info(f"Successfully loaded SLA configuration from {sla_file_path}")
        except FileNotFoundError:
            logging.warning(f"SLA file not found at {sla_file_path}. SLA violation rate will not be calculated.")
            self.sla_df = pd.DataFrame()

    def collect(self, end_time, duration, limit, service, task_type):
        request_data = {
            "start": int((end_time - int(duration)) * 1000000),
            "end": int(end_time * 1000000),
            "operation": task_type,
            "limit": limit,
            "service": service,
            "tags": '{"http.status_code":"200"}'
        }

        response = requests.get(self.endpoint, params=request_data)
        self.traces = json.loads(response.content)["data"]

        return self.traces

    def calculate_duration_difference(self, row, grouped_children):
        if row["childProcessId"] == "NoChild":
            return row["parentDuration"]
        same_parent_duration_sum = grouped_children.get(row["parentId"], 0)
        return row["parentDuration"] - same_parent_duration_sum

    @staticmethod
    def _union_duration(intervals):
        intervals = sorted((float(start), float(end)) for start, end in intervals
                           if np.isfinite(start) and np.isfinite(end) and end > start)
        if not intervals:
            return 0.0
        covered = 0.0
        current_start, current_end = intervals[0]
        for start, end in intervals[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                covered += current_end - current_start
                current_start, current_end = start, end
        return covered + current_end - current_start

    def _exclusive_parent_durations(self, merged_df):
        exclusive, overlap_parents, naive_negative_parents = {}, 0, 0
        for key, group in merged_df.groupby(["traceId", "parentId"], sort=False):
            parent_duration = float(group["parentDuration"].iloc[0])
            parent_start = float(group["parentStartTime"].iloc[0])
            parent_end = parent_start + parent_duration
            intervals, naive_sum = [], 0.0
            for _, child in group[group["childProcessId"] != "NoChild"].iterrows():
                child_start = float(child["startTime"])
                child_duration = float(child["childDuration"])
                naive_sum += child_duration
                start = max(parent_start, child_start)
                end = min(parent_end, child_start + child_duration)
                if end > start:
                    intervals.append((start, end))
            union_duration = self._union_duration(intervals)
            overlap_parents += int(union_duration + 1e-9 < naive_sum)
            naive_negative_parents += int(naive_sum > parent_duration)
            exclusive[key] = max(0.0, parent_duration - union_duration)
        return exclusive, overlap_parents, naive_negative_parents

    @staticmethod
    def _unique_cross_region_child_penalties(merged_df, mask, network_latency_us):
        unique_children = (
            merged_df.loc[
                mask & (merged_df["childId"] != "NoChild"),
                ["traceId", "parentId", "childId"],
            ]
            .drop_duplicates(["traceId", "parentId", "childId"])
        )
        penalties = (
            unique_children
            .groupby(["traceId", "parentId"], as_index=False)
            .size()
            .rename(columns={"size": "cross_region_child_count"})
        )
        penalties["total_latency_penalty"] = (
            penalties["cross_region_child_count"] * int(network_latency_us)
        )
        return penalties, unique_children

    @staticmethod
    def _span_kind(tags):
        if not isinstance(tags, list):
            return "unknown"
        for tag in tags:
            if isinstance(tag, dict) and tag.get("key") == "span.kind":
                return str(tag.get("value", "unknown")).strip().lower()
        return "unknown"

    def process_trace_data(self):
        raw_trace = self.traces
        if raw_trace == []:
            print("Warning: No trace data collected")
            return pd.DataFrame()

        service_id_mapping = (
            pd.json_normalize(raw_trace)
            .filter(regex="serviceName|traceID|tags")
            .rename(
                columns=lambda x: re.sub(
                    r"processes\.(.*)\.serviceName|processes\.(.*)\.tags",
                    lambda match_obj: match_obj.group(1)
                    if match_obj.group(1)
                    else f"{match_obj.group(2)}Pod",
                    x,
                )
            )
            .rename(columns={"traceID": "traceId"})
        )

        service_id_mapping = (
            service_id_mapping.filter(regex=".*Pod")
            .applymap(
                lambda x: [v["value"] for v in x if v["key"] == "hostname"][0]
                if isinstance(x, list)
                else ""
            )
            .combine_first(service_id_mapping)
        )

        spans_data = pd.json_normalize(raw_trace, record_path="spans")[
            [
                "traceID",
                "spanID",
                "operationName",
                "duration",
                "processID",
                "references",
                "startTime",
            ]
        ]

        spans_with_parent = spans_data[~(spans_data["references"].astype(str) == "[]")]

        root_spans = spans_data[(spans_data["references"].astype(str) == "[]")]

        root_spans = root_spans.rename(
            columns={
                "traceID": "traceId",
                "startTime": "traceTime",
                "duration": "traceLatency"
            }
        )[["traceId", "traceTime", "traceLatency"]]

        spans_with_parent.loc[:, "parentId"] = spans_with_parent["references"].map(
            lambda x: x[0]["spanID"]
        )

        temp_parent_spans = spans_data[
            ["traceID", "spanID", "operationName", "duration", "processID", "startTime"]
        ].rename(
            columns={
                "spanID": "parentId",
                "processID": "parentProcessId",
                "operationName": "parentOperation",
                "duration": "parentDuration",
                "startTime": "parentStartTime",
                "traceID": "traceId",
            }
        )
        temp_children_spans = spans_with_parent[
            [
                "operationName",
                "duration",
                "parentId",
                "traceID",
                "spanID",
                "processID",
                "startTime",
            ]
        ].rename(
            columns={
                "spanID": "childId",
                "processID": "childProcessId",
                "operationName": "childOperation",
                "duration": "childDuration",
                "traceID": "traceId",
            }
        )

        merged_df = pd.merge(
            temp_parent_spans, temp_children_spans, on=["parentId", "traceId"], how="left"
        )

        merged_df = merged_df[
            [
                "traceId",
                "childOperation",
                "childDuration",
                "parentOperation",
                "parentDuration",
                "parentStartTime",
                "parentId",
                "childId",
                "parentProcessId",
                "childProcessId",
                "startTime",
            ]
        ]

        merged_df = merged_df.merge(service_id_mapping, on="traceId")
        merged_df = merged_df.merge(root_spans, on="traceId")
        merged_df = merged_df.fillna("NoChild")

        merged_df = merged_df.assign(
            childMS=merged_df.apply(
                lambda x: x[x["childProcessId"]] if x["childProcessId"] != "NoChild" else "NoChild",
                axis=1,
            ),
            childPod=merged_df.apply(
                lambda x: x[f"{str(x['childProcessId'])}Pod"] if x["childProcessId"] != "NoChild" else "NoChildPod",
                axis=1,
            ),
            parentMS=merged_df.apply(
                lambda x: x[x["parentProcessId"]],
                axis=1,
            ),
            parentPod=merged_df.apply(
                lambda x: x[f"{str(x['parentProcessId'])}Pod"],
                axis=1,
            ),
            endTime=merged_df.apply(
                lambda x: x["startTime"] + x["childDuration"] if x["childProcessId"] != "NoChild" else "NoChild",
                axis=1,
            ),
        )

        exclusive, overlap_parents, naive_negative_parents = self._exclusive_parent_durations(merged_df)
        merged_df["durationDifference"] = [
            exclusive[(trace_id, parent_id)]
            for trace_id, parent_id in zip(merged_df["traceId"], merged_df["parentId"])
        ]

        merged_df = merged_df[
            [
                "traceId",
                "traceTime",
                "startTime",
                "endTime",
                "parentId",
                "childId",
                "childOperation",
                "parentOperation",
                "childMS",
                "childPod",
                "parentMS",
                "parentPod",
                "parentDuration",
                "childDuration",
                "durationDifference",
            ]
        ]

        merged_df.attrs["delay_correction_stats"] = {
            "mode": "queueflower_raw_no_network_subtraction",
            "exclusive_time": "parent_duration_minus_union_of_direct_child_intervals",
            "parallel_or_clipped_parent_spans": int(overlap_parents),
            "parents_that_naive_sum_would_make_negative": int(naive_negative_parents),
            "negative_duration_floor_us": 0,
        }
        return merged_df

    def calculate_average_latency(self,task_name:str):
        trace_durations = []
        for trace in self.traces:
            trace_duration = max([int(span["duration"]) for span in trace["spans"]])
            trace_durations.append(trace_duration)
        if len(trace_durations) > 0:
            trace_durations = np.array(trace_durations)
            trace_durations.sort()
            average_latency = sum(trace_durations) / len(trace_durations) if len(trace_durations) != 0 else 0

            filtered_trace_durations = trace_durations[:int(len(trace_durations)*0.9)]
            average_normal_latency = sum(filtered_trace_durations) / len(filtered_trace_durations) if len(filtered_trace_durations) != 0 else 0

            tail_trace_durations = trace_durations[int(len(trace_durations)*0.9):]
            tail_latency = sum(tail_trace_durations) / len(tail_trace_durations)

            sla_violations_rate = 0.0

            if not self.sla_df.empty and task_name in self.sla_df.columns:
                sla_value = self.sla_df[task_name].iloc[0]
                sla_violation_traces = [d for d in trace_durations if d >= sla_value]
                sla_violations_rate = len(sla_violation_traces) / len(trace_durations) if len(sla_violation_traces) != 0 else 0

            else:
                logging.warning(f"SLA for task '{task_name}' not found in SLA file. Cannot calculate violation rate.")

            return average_latency, average_normal_latency, tail_latency, sla_violations_rate
        else:
            logging.warning("No traces found for latency calculation.")
            return 0, 0, 0, 0

    def get_all_latency(self):
        trace_durations = []
        for trace in self.traces:
            trace_duration = max([int(span["duration"]) for span in trace["spans"]])
            trace_durations.append(trace_duration)
        return trace_durations

    def clear(self):
        self.traces = None

