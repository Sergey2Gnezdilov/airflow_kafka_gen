from kubernetes.client import models as k8s

namespace = "scheduler"
tolerations = [{"effect": "NoSchedule", "operator": "Exists"}]
affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(
                weight=1,
                preference=k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(key="node-role.kubernetes.io", operator="In", values=["ops"])
                    ]
                ),
            )
        ]
    )
)

pod_params = {
    "quarter": k8s.V1ResourceRequirements(limits={"cpu": "250m", "memory": "256M"}),
    "quarter_2x_mem": k8s.V1ResourceRequirements(limits={"cpu": "250m", "memory": "512M"}),
    "quarter_4x_mem": k8s.V1ResourceRequirements(limits={"cpu": "250m", "memory": "1G"}),
    "quarter_16x_mem": k8s.V1ResourceRequirements(limits={"cpu": "250m", "memory": "4G"}),
    "half": k8s.V1ResourceRequirements(limits={"cpu": "500m", "memory": "512M"}),
    "half_2x_mem": k8s.V1ResourceRequirements(limits={"cpu": "500m", "memory": "1G"}),
    "half_4x_mem": k8s.V1ResourceRequirements(limits={"cpu": "500m", "memory": "2G"}),
    "half_8x_mem": k8s.V1ResourceRequirements(limits={"cpu": "500m", "memory": "4G"}),
    "half_16x_mem": k8s.V1ResourceRequirements(limits={"cpu": "500m", "memory": "8G"}),
    "one": k8s.V1ResourceRequirements(limits={"cpu": "1000m", "memory": "1G"}),
    "one_2x_mem": k8s.V1ResourceRequirements(limits={"cpu": "1000m", "memory": "2G"}),
    "one_4x_mem": k8s.V1ResourceRequirements(limits={"cpu": "1000m", "memory": "4G"}),
    "one_8x_mem": k8s.V1ResourceRequirements(limits={"cpu": "1000m", "memory": "8G"}),
    "double": k8s.V1ResourceRequirements(limits={"cpu": "2000m", "memory": "2G"}),
    "double_2x_mem": k8s.V1ResourceRequirements(limits={"cpu": "2000m", "memory": "4G"}),
    "double_4x_mem": k8s.V1ResourceRequirements(limits={"cpu": "2000m", "memory": "8G"}),
}
