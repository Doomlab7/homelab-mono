locals {
  domain                       = "pype.dev"
  cloudflare_pages_name        = "pype-dev"
  github_owner                 = "pypeaday"
  cloudflare_pages_target_repo = local.domain
  cloudflare_pages_root_dir    = "/"
  account_id                   = var.cloudflare_account_id
  production_branch            = "markout"
  preview_branch               = "main"
}
