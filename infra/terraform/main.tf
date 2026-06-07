terraform {
  required_version = ">= 1.5"

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.45"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

provider "hcloud" {
  token = var.hcloud_token
}

# SSH key uploaded to Hetzner so Terraform (and you) can reach the server.
resource "hcloud_ssh_key" "workshop" {
  name       = "${var.server_name}-key"
  public_key = file(pathexpand(var.ssh_public_key_path))
}

# Firewall: SSH, the k3s/Kubernetes API, and the Kafka external listener.
# Lock `allowed_cidrs` down to your own IP — see variables.tf.
resource "hcloud_firewall" "workshop" {
  name = "${var.server_name}-fw"

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22" # SSH
    source_ips = var.allowed_cidrs
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "6443" # k3s / Kubernetes API
    source_ips = var.allowed_cidrs
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "32094" # Strimzi external listener (NodePort, see kafka/kafka-cluster.yaml)
    source_ips = var.allowed_cidrs
  }
}

# cloud-init: install k3s on first boot.
# - reads the server's own public IP from Hetzner metadata and adds it as a TLS SAN
#   so the kubeconfig works from your laptop
# - --write-kubeconfig-mode 644 lets us copy the kubeconfig off the box
locals {
  cloud_init = <<-EOT
    #cloud-config
    package_update: true
    runcmd:
      - PUBLIC_IP=$(curl -s http://169.254.169.254/hetzner/v1/metadata/public-ipv4)
      - curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--write-kubeconfig-mode 644 --tls-san $PUBLIC_IP" sh -
  EOT
}

resource "hcloud_server" "workshop" {
  name         = var.server_name
  server_type  = var.server_type
  image        = var.image
  location     = var.location
  ssh_keys     = [hcloud_ssh_key.workshop.id]
  firewall_ids = [hcloud_firewall.workshop.id]
  user_data    = local.cloud_init

  labels = {
    project = "kafka-workshop"
  }
}

# After the server is up, pull the k3s kubeconfig and rewrite its server URL
# (127.0.0.1 -> public IP) so `kubectl` works from your machine.
# Writes ./kubeconfig, which outputs.tf exposes via `terraform output -raw kubeconfig`.
resource "null_resource" "kubeconfig" {
  depends_on = [hcloud_server.workshop]

  triggers = {
    server_id = hcloud_server.workshop.id
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      for i in $(seq 1 30); do
        scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 \
          root@${hcloud_server.workshop.ipv4_address}:/etc/rancher/k3s/k3s.yaml ${path.module}/kubeconfig 2>/dev/null && break
        echo "waiting for k3s kubeconfig... attempt $i/30"; sleep 10
      done
      sed -i.bak "s/127.0.0.1/${hcloud_server.workshop.ipv4_address}/g" ${path.module}/kubeconfig
      rm -f ${path.module}/kubeconfig.bak
      echo "kubeconfig written to ${path.module}/kubeconfig"
    EOT
  }
}
