import subprocess
import datetime
import os
import csv
import subprocess
import threading
import signal  
import shlex   
import time
class WorkloadGenerator:
    def __init__(self, endpoint: str="5000", rate: int=1000, duration: str="15m") -> None:
        self.threads = 15 
        self.connections = 400 
        self.timeout = 10 
        self.duration = duration
        self.rate = rate
        self.script = "./wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua"
        self.endpoint = f"http://10.112.26.188:{endpoint}"
        
        self.command = f"wrk -t {self.threads} -c {self.connections} -T {self.timeout}s  \
                -L -s {self.script} {self.endpoint}"
        
        self.workload_process = None 
        self.process_ready_event = threading.Event()
        self.stop_requested_event = threading.Event()
    
    def generate_stationary(self,duration,rate):
        if self.stop_requested_event.is_set():
            print(datetime.datetime.now(), "[Workload] Stationary run cancelled before start.")
            return
        command_str = f"{self.command} -d {duration} -R {rate}"
        command_list = shlex.split(command_str)
        print(datetime.datetime.now(), f"[Workload] Starting with {rate} reqs/s for {duration}..")
        try:
            self.workload_process = subprocess.Popen(
                command_list, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True  
            )
            self.process_ready_event.set()
            output, error = self.workload_process.communicate()
            if self.stop_requested_event.is_set():
                print(datetime.datetime.now(), "[Workload] Stationary process was interrupted.")
            if error and self.workload_process.returncode != 0:
                print(datetime.datetime.now(), "[Workload] Error:", error)
            else:
                print(datetime.datetime.now(), "[Workload] Done (or Interrupted):")
                print(output)

        except Exception as e:
            print(datetime.datetime.now(), f"[Workload] Popen failed: {e}")
        finally:
            self.process_ready_event.clear()
            self.stop_requested_event.clear() 
            self.workload_process = None
            print(datetime.datetime.now(), "[Workload] Thread cleanup complete.")
            
    def generate_nonstationary(self, pattern_file: str = "./input/pattern_4.csv"):
        
        print(datetime.datetime.now(), f"[Workload] Starting non-stationary from: {pattern_file}")
        
        try:
            if not os.path.exists(pattern_file):
                print(datetime.datetime.now(), f"[Workload] Error: Pattern file not found at {pattern_file}")
                return

            with open(pattern_file, 'r') as f:
                reader = csv.reader(f)
                
                for i, row in enumerate(reader):
                    if self.stop_requested_event.is_set():
                        print(datetime.datetime.now(), "[Workload] Stop request received. Halting non-stationary loop.")
                        break
                    
                    try:
                        if len(row) < 2:
                            print(datetime.datetime.now(), f"[Workload] Skipping malformed row {i+1}: {row}")
                            continue
                        step_duration = row[0].strip()
                        step_rate = int(row[1].strip())
                    except ValueError as e:
                        print(datetime.datetime.now(), f"[Workload] Skipping bad data in row {i+1}: {row} | Error: {e}")
                        continue
                    
                    print(datetime.datetime.now(), f"[Workload] Step {i+1}: {step_rate} reqs/s for {step_duration}...")
                    
                    command_str = f"{self.command} -d {step_duration} -R {step_rate}"
                    command_list = shlex.split(command_str)
                    
                    try:
                        self.workload_process = subprocess.Popen(
                            command_list,
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE,
                            text=True
                        )
                        self.process_ready_event.set()

                        output, error = self.workload_process.communicate()

                        if error and self.workload_process.returncode != 0:
                            print(datetime.datetime.now(), f"[Workload] Step {i+1} Error:", error)
                        else:
                            print(datetime.datetime.now(), f"[Workload] Step {i+1} Done:")
                            print(output)
                            
                    except Exception as e:
                        print(datetime.datetime.now(), f"[Workload] Popen failed for step {i+1}: {e}")
                    finally:
                        self.process_ready_event.clear()
                        self.workload_process = None

        except Exception as e:
            print(datetime.datetime.now(), f"[Workload] Fatal error reading pattern file: {e}")
        finally:
            self.stop_requested_event.clear()
            print(datetime.datetime.now(), "[Workload] Non-stationary thread cleanup complete.")


    def stop_workload(self):
        print(datetime.datetime.now(), "[Main] Attempting to stop workload...")
        self.stop_requested_event.set()
        if not self.process_ready_event.wait(timeout=1):
            print(datetime.datetime.now(), "[Main] No sub-process is currently running. Loop will stop on next check.")
            return

        if self.workload_process:
            try:
                self.workload_process.send_signal(signal.SIGINT)
                print(datetime.datetime.now(), "[Main] Sent SIGINT to current wrk process.")
            except Exception as e:
                print(datetime.datetime.now(), f"[Main] Error sending signal (process may have just finished): {e}")
        else:
            print(datetime.datetime.now(), "[Main] Process just finished. Loop will stop on next check.")


    def terminate(self):
        self.workload_process.terminate()
        output, error = self.workload_process.communicate()
        if error.decode():
            print(datetime.datetime.now(), "[Workload] Error:", error.decode())
        else:
            print(datetime.datetime.now(), "[Workload] Done:", output.decode())
