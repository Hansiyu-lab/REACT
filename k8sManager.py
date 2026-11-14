from kubernetes import client, config
from time import sleep, time
import datetime
from kubernetes.client import V1ResourceRequirements 
import os
import logging
import subprocess
import json

class K8sManager:
    def __init__(self, namespace):
        config.load_kube_config()

        self.api_client_corev1 = client.CoreV1Api()
        self.api_client_appsv1 = client.AppsV1Api()
        self.namespace = namespace

        self.pod_list = self.api_client_corev1.list_namespaced_pod(
            namespace=self.namespace)
        self.service_list = self.api_client_corev1.list_namespaced_service(
            namespace=self.namespace)
        self.deployment_list = self.api_client_appsv1.list_namespaced_deployment(
            namespace=self.namespace)

    def update(self):
        self.pod_list = self.api_client_corev1.list_namespaced_pod(
            namespace=self.namespace)
        self.service_list = self.api_client_corev1.list_namespaced_service(
            namespace=self.namespace)
        self.deployment_list = self.api_client_appsv1.list_namespaced_deployment(
            namespace=self.namespace)

    def get_pods_name_list(self):
        return [pod.metadata.name for pod in self.pod_list.items]
    def show_deployments(self):
        for deployment in self.deployment_list.items:
            print(deployment.metadata.name)
    def prepare_restart_body(self, deployment):
        now = datetime.datetime.utcnow().isoformat() + "Z"
        annotations = {
            "kubectl.kubernetes.io/restartedAt": now
        }
        
        if deployment.spec.template.metadata.annotations is None:
            deployment.spec.template.metadata.annotations = {}
            
        deployment.spec.template.metadata.annotations.update(annotations)
        return deployment
    
    def update_deployment(self, deployment_name, replicas=None, cpu_request=None, cpu_limit=None, trigger_restart=False):
        try:
            deployment = self.api_client_appsv1.read_namespaced_deployment(
                deployment_name, self.namespace)

            old_replicas = deployment.spec.replicas
            container = deployment.spec.template.spec.containers[0]
            old_cpu_request = "N/A"
            old_cpu_limit = "N/A"
            if container.resources and container.resources.requests:
                old_cpu_request = container.resources.requests.get("cpu", "Unrequested")
            if container.resources and container.resources.limits:
                old_cpu_limit = container.resources.limits.get("cpu", "Unlimited")

            if replicas is not None:
                deployment.spec.replicas = replicas
            if cpu_request is not None and cpu_limit is not None:
                if not container.resources:
                    container.resources = V1ResourceRequirements()
                if not container.resources.requests:
                    container.resources.requests = {}
                if not container.resources.limits:
                    container.resources.limits = {}
                container.resources.requests["cpu"] = f"{cpu_request}"
                container.resources.limits["cpu"] = f"{cpu_limit}"

            if trigger_restart:
                deployment = self.prepare_restart_body(deployment)

            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)

            print(f"{datetime.datetime.now()} [K8sManager Update] Atomically updated {deployment_name}:")
            if replicas is not None:
                print(f"  - Replicas: {old_replicas} -> {replicas}")
            if cpu_request is not None:
                print(f"  - CPU Request: {old_cpu_request} -> {cpu_request}")
            if cpu_limit is not None:
                print(f"  - CPU Limit:   {old_cpu_limit} -> {cpu_limit}")
            if trigger_restart:
                print("  - Restart triggered.")

        except Exception as e:
            print(f"Error updating deployment {deployment_name}: {e}")

    def edge_prior_scale_deployment(k8s_manager, pod_on_node):

        EDGE_CAPACITY_LIMITS = {
        "frontend": 6,       
        "geo": 2,            
        "profile": 2,
        "rate": 2,
        "recommendation": 2, 
        "reservation": 2,
        "search": 4,
        "user": 2
        }
        for service_name, total_required in pod_on_node.items():
            limit = EDGE_CAPACITY_LIMITS.get(service_name, 4)
            
            if total_required <= limit:
                edge_replicas = total_required
                cloud_replicas = 0
            else:
                edge_replicas = limit
                cloud_replicas = total_required - limit
                
            edge_replicas = max(1, int(edge_replicas))
            cloud_replicas = max(0, int(cloud_replicas)) 
            
            k8s_manager.scale_deployment(f"{service_name}-edge", int(edge_replicas))
            k8s_manager.scale_deployment(f"{service_name}-cloud", int(cloud_replicas))
        
    def scale_deployment(self, deployment_name, replica_num):
        deployment = self.api_client_appsv1.read_namespaced_deployment(
            deployment_name, self.namespace)
        
        if deployment.spec.replicas != replica_num:
            print(f"Start scaling {deployment_name} to {replica_num}...")
            
            deployment.spec.replicas = replica_num
            self.api_client_appsv1.patch_namespaced_deployment_scale(
                    name=deployment_name, namespace=self.namespace, body=deployment)
            
            while True:
                deployment = self.api_client_appsv1.read_namespaced_deployment(
                    deployment_name, self.namespace)
                
                current_replicas = deployment.status.replicas or 0
                current_available = deployment.status.available_replicas or 0
                current_updated = deployment.status.updated_replicas or 0

                if (
                    current_available == replica_num
                    and current_replicas == replica_num
                    and current_updated == replica_num
                ):
                    break
        
                sleep(1) 

            print(datetime.datetime.now(),
                f"[K8sManager Scale] Scale {deployment_name} to {replica_num} pods.")
        else:
            print(datetime.datetime.now(),
                f"[K8sManager Scale] Keep {deployment_name} have {replica_num} pods.")
            
    def set_limit(self, deployment_name, cpu_limit):
        deployment = self.api_client_appsv1.read_namespaced_deployment(
            deployment_name, self.namespace)
        if deployment.spec.template.spec.containers[0].resources.limits:
            old_cpu_limit = deployment.spec.template.spec.containers[0].resources.limits["cpu"]
            deployment.spec.template.spec.containers[0].resources.limits["cpu"] = f"{cpu_limit}"
            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
            print(datetime.datetime.now(),
                f"[K8sManager Limit] Set {deployment_name} limit cpu from {old_cpu_limit} to {cpu_limit}.")
        else:
            deployment.spec.template.spec.containers[0].resources.limits = {"cpu": f"{cpu_limit}"}
            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
            print(datetime.datetime.now(),
                f"[K8sManager Limit] Set {deployment_name} limit cpu from Unlimited to {cpu_limit}.")
        sleep(2)

    def set_request(self, deployment_name, cpu_request):
        deployment = self.api_client_appsv1.read_namespaced_deployment(
            deployment_name, self.namespace)
        if deployment.spec.template.spec.containers[0].resources.requests:
            old_cpu_request = deployment.spec.template.spec.containers[0].resources.requests["cpu"]
            deployment.spec.template.spec.containers[0].resources.requests["cpu"] = f"{cpu_request}"
            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
            print(datetime.datetime.now(),
                f"[K8sManager request] Set {deployment_name} request cpu from {old_cpu_request} to {cpu_request}.")
        else:
            deployment.spec.template.spec.containers[0].resources.requests = {"cpu": f"{cpu_request}"}
            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
            print(datetime.datetime.now(),
                f"[K8sManager Request] Set {deployment_name} request cpu from Unrequested to {cpu_request}.")
        sleep(2)
    
    def set_cpu_resources(self, deployment_name, cpu_request, cpu_limit):
        try:
            deployment = self.api_client_appsv1.read_namespaced_deployment(
                deployment_name, self.namespace)
            
            container = deployment.spec.template.spec.containers[0]

            if not container.resources:
                container.resources = V1ResourceRequirements()
            if not container.resources.requests:
                container.resources.requests = {}
            if not container.resources.limits:
                container.resources.limits = {}

            old_cpu_request = container.resources.requests.get("cpu", "Unrequested")
            old_cpu_limit = container.resources.limits.get("cpu", "Unlimited")

            container.resources.requests["cpu"] = f"{cpu_request}"
            container.resources.limits["cpu"] = f"{cpu_limit}"

            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)

            print(f"{datetime.datetime.now()} [K8sManager Resources] Atomically updated {deployment_name} CPU:")
            print(f"  - Request: {old_cpu_request} -> {cpu_request}")
            print(f"  - Limit:   {old_cpu_limit} -> {cpu_limit}")

        except Exception as e:
            print(f"Error updating resources for {deployment_name}: {e}")
        
    def set_restart(self, deployment_name):
        deployment = self.api_client_appsv1.read_namespaced_deployment(
            deployment_name, self.namespace)
        deployment.spec.template.spec.restart_policy = "Always"
        self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
        print(datetime.datetime.now(),
                f"[K8sManager Restart Policy] Set {deployment_name} restart policy to True.")
        sleep(2)
    
    def apply_hpa_dynamic(self):

        EDGE_CAPACITY_LIMITS = {
            "frontend": 6,
            "geo": 2,
            "profile": 2,
            "rate": 2,
            "recommendation": 2,
            "reservation": 2,
            "search": 4,
            "user": 2
        }

        base_services = ["frontend", "geo", "profile", "rate", "search", "reservation", "recommendation", "user"]
        suffixes = ["-edge", "-cloud"]

        print(f"Applying HPA rules with Edge-Priority strategy...")

        for svc in base_services:

            edge_limit = EDGE_CAPACITY_LIMITS.get(svc, 4)

            for suffix in suffixes:
                deployment_name = f"{svc}{suffix}"
                hpa_name = f"{svc}{suffix}-hpa"
                current_min = 1
                current_max = edge_limit if suffix == "-edge" else 30

                cmd = f"""
cat <<EOF | kubectl apply -f -
apiVersion: autoscaling/v1
kind: HorizontalPodAutoscaler
metadata:
  name: {hpa_name}
  namespace: hotel
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {deployment_name}
  minReplicas: {current_min}
  maxReplicas: {current_max}
  targetCPUUtilizationPercentage: 60
EOF
"""

                os.system(cmd + " > /dev/null 2>&1")

                print(f"  -> Set HPA for {deployment_name}: Min={current_min}, Max={current_max}, TargetCPU=60%")

        print(f"HPA initialization complete.")
        
    def delete_all_hpa(self):
        logging.info("Deleting all HPA resources...")
        os.system("kubectl delete hpa --all -n hotel") 
        
    def get_current_replicas_map(self):

        base_services = ["frontend", "geo", "profile", "rate", "search", "reservation", "recommendation", "user"]
        suffixes = ["-edge", "-cloud"]
        current_status = {svc: 0 for svc in base_services}
        
        cmd = "kubectl get deployments -n hotel -o json"
        
        try:
            output = subprocess.check_output(cmd, shell=True)
            data = json.loads(output)
            
            for item in data.get('items', []):
                deploy_name = item['metadata']['name']
                count = item['status'].get('replicas', 0)
                
                for svc in base_services:
                    if deploy_name == f"{svc}-edge" or deploy_name == f"{svc}-cloud":
                        current_status[svc] += count
                        break
        
        except subprocess.CalledProcessError as e:
            logging.error(f"Kubectl command failed with return code {e.returncode}: {e}")     
        except Exception as e:
            logging.error(f"Failed to get deployment status: {e}")
            
        return current_status