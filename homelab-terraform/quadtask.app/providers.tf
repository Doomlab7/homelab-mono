terraform {
  # for open tofu
  required_version = ">= 1.9.0"

  backend "s3" {
    endpoint = "https://s3.paynepride.com"
    bucket   = "infrastructure-state"
    key      = "quadtask.app/terraform.tfstate"
    region   = "us-east-1"
    # access_key                  = "ACCESSKEY"
    # secret_key                  = "SECRETKEY"
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    use_path_style              = true
  }
  required_providers {
    namecheap = {
      source  = "namecheap/namecheap"
      version = ">= 2.0.0"

    }
    namecheapecosystem = {
      source  = "Namecheap-Ecosystem/namecheap"
      version = "0.1.7"
    }
    github = {
      source  = "integrations/github"
      version = "6.2.1"

    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "4.41.0"
    }

  }
}

provider "cloudflare" {
  api_token = var.cloudflare_api_token
}

provider "namecheapecosystem" {
  username  = var.namecheap_username
  api_user  = var.namecheap_username
  api_token = var.namecheap_api_key
}
