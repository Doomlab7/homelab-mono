locals {
  domain                       = "quadtask.app"
  github_owner                 = "pypeaday"
  cloudflare_pages_target_repo = local.domain
  account_id                   = var.cloudflare_account_id
}
