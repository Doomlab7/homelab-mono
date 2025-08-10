resource "cloudflare_zone" "zone" {
  # account_id = data.cloudflare_accounts.cloudflare_account_data.id
  account_id = local.account_id
  zone       = local.domain
}

# Cloudflare Pages project with managing build config
resource "cloudflare_pages_project" "build_config" {
  # account_id = data.cloudflare_accounts.cloudflare_account_data.id
  account_id = local.account_id

  name              = local.cloudflare_pages_name
  production_branch = local.production_branch
  source {
    type = "github"
    config {
      owner                         = local.github_owner
      repo_name                     = local.cloudflare_pages_target_repo
      production_branch             = local.production_branch
      production_deployment_enabled = true
      preview_branch_includes       = [local.preview_branch]
    }
  }
  build_config {
    # root_dir = local.cloudflare_pages_root_dir
  }
  deployment_configs {
    preview {
      fail_open   = true
      usage_model = "standard"
    }
    production {
      fail_open   = true
      usage_model = "standard"
    }
  }
}
resource "cloudflare_pages_domain" "cf_domain" {
  # account_id = data.cloudflare_accounts.cloudflare_account_data.id
  account_id   = local.account_id
  project_name = cloudflare_pages_project.build_config.name
  domain       = local.domain
}

# NOTE: I think without this that www.pype.dev times out
resource "cloudflare_pages_domain" "cf_domain_www" {
  account_id   = local.account_id
  project_name = cloudflare_pages_project.build_config.name
  domain       = "www.${local.domain}"
}

# resource "cloudflare_record" "zyx" {
#   zone_id = cloudflare_zone.zone.id
#
#   name            = "zyx"
#   type            = "CNAME"
#   proxied         = true
#   ttl             = 1
#   allow_overwrite = false
#   content         = local.domain
#
#   # data {
#   #   content = local.domain
#   # }
# }

resource "cloudflare_record" "root" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "CNAME"
  proxied         = true
  ttl             = 1
  allow_overwrite = false
  content         = "${local.cloudflare_pages_name}.pages.dev"

  # data {
  #   target = "${local.cloudflare_pages_name}.pages.dev"
  # }
}

resource "cloudflare_record" "www" {
  zone_id = cloudflare_zone.zone.id

  name            = "www"
  type            = "CNAME"
  proxied         = true
  ttl             = 1
  allow_overwrite = false

  content = local.domain
}
resource "cloudflare_record" "protonmail" {
  zone_id = cloudflare_zone.zone.id

  name            = "protonmail._domainkey"
  content         = var.protonmail_verification_key1
  type            = "CNAME"
  proxied         = false
  ttl             = 1
  allow_overwrite = false
}
resource "cloudflare_record" "protonmail2" {
  zone_id         = cloudflare_zone.zone.id
  name            = "protonmail2._domainkey"
  content         = var.protonmail_verification_key2
  type            = "CNAME"
  proxied         = false
  ttl             = 1
  allow_overwrite = false
}

resource "cloudflare_record" "protonmail3" {
  zone_id = cloudflare_zone.zone.id

  name            = "protonmail3._domainkey"
  content         = var.protonmail_verification_key3
  type            = "CNAME"
  proxied         = false
  ttl             = 1
  allow_overwrite = false
}
resource "cloudflare_record" "mx20" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "MX"
  proxied         = false
  ttl             = 1
  allow_overwrite = false
  priority        = 20

  data {
    target   = "mailsec.protonmail.ch"
    priority = 20
  }
}
resource "cloudflare_record" "mx10" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "MX"
  proxied         = false
  ttl             = 1
  allow_overwrite = false
  priority        = 10

  data {
    target   = "mail.protonmail.ch"
    priority = 10
  }
}
resource "cloudflare_record" "dmarc" {
  zone_id = cloudflare_zone.zone.id

  name            = "_dmarc"
  type            = "TXT"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = "v=DMARC1; p=none;"
}
resource "cloudflare_record" "spf" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "TXT"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = "v=spf1 include:_spf.protonmail.ch mx include:spf.efwd.registrar-servers.com ~all"
}
resource "cloudflare_record" "txt_protonmail_verification" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "TXT"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = "protonmail-verification=492d357f4cbaa76cc5962cc3a065da5dea4bc711"
}
resource "cloudflare_record" "txt_simplelogin_verification" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "TXT"
  proxied         = false
  ttl             = 1
  allow_overwrite = false
  content         = "sl-verification=esmctdubavptsmklzsewyjzzkxgdrm"
}
resource "cloudflare_record" "txt_google_verification" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "TXT"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = "google-site-verification=dMUQLxVo2lv2G5qLUC3oo8rAnwFg5XFHWMKQVKFEDns"
}

################################################################################

# Create a new Namecheap domain DNS
resource "namecheap_domain_dns" "mydns" {
  provider = namecheapecosystem
  domain   = local.domain

  nameservers = cloudflare_zone.zone.name_servers
}
