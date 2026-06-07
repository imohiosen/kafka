output "server_ipv4" {
  description = "Public IPv4 address of the k3s server."
  value       = hcloud_server.workshop.ipv4_address
}

output "ssh_command" {
  description = "Convenience SSH command."
  value       = "ssh root@${hcloud_server.workshop.ipv4_address}"
}

output "kubeconfig" {
  description = "Kubeconfig for the k3s cluster. Use: terraform output -raw kubeconfig > ~/.kube/kafka-workshop.yaml"
  value       = fileexists("${path.module}/kubeconfig") ? file("${path.module}/kubeconfig") : "run 'terraform apply' first, then re-run 'terraform output'"
  sensitive   = true
}
