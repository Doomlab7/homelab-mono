locals {
  domain                       = "mydigitalharbor.com"
  pages_domain                 = "mydigitalharbor-com.pages.dev"
  github_owner                 = "pypeaday"
  cloudflare_pages_target_repo = local.domain
  account_id                   = var.cloudflare_account_id
}
