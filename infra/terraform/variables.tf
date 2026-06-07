variable "hcloud_token" {
  description = "Hetzner Cloud API token (create one in the Hetzner console under Security > API tokens)."
  type        = string
  sensitive   = true
}

variable "server_name" {
  description = "Name/prefix for the server and related resources."
  type        = string
  default     = "kafka-workshop"
}

variable "server_type" {
  description = "Hetzner server type. cx22 = 2 shared vCPU / 4 GB RAM, a fine starting point for the workshop."
  type        = string
  default     = "cx22"
}

variable "location" {
  description = "Hetzner location (e.g. nbg1, fsn1, hel1, ash, hil)."
  type        = string
  default     = "nbg1"
}

variable "image" {
  description = "Base OS image."
  type        = string
  default     = "ubuntu-24.04"
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key uploaded to Hetzner. Its matching private key is used to fetch the kubeconfig."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "allowed_cidrs" {
  description = "CIDRs allowed to reach SSH / k3s API / Kafka. IMPORTANT: lock this to your own IP, e.g. [\"203.0.113.4/32\"]."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
