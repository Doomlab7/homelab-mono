resource "cloudflare_zone" "zone" {
  # account_id = data.cloudflare_accounts.cloudflare_account_data.id
  account_id = local.account_id
  zone       = local.domain
}

resource "cloudflare_record" "www" {
  zone_id = cloudflare_zone.zone.id

  name            = "www"
  type            = "CNAME"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  # Grabbed from fly dashboard. might need to manually update this
  # content = "66.241.125.33"
  content = local.domain
}

resource "cloudflare_record" "root" {
  zone_id = cloudflare_zone.zone.id

  name            = "@"
  type            = "A"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  # Grabbed from fly dashboard. might need to manually update this
  content = "66.241.125.33"
}

resource "cloudflare_record" "root6" {
  zone_id = cloudflare_zone.zone.id

  name            = "@"
  type            = "AAAA"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  # Grabbed from fly dashboard. might need to manually update this
  content = "2a09:8280:1::58:1452:0"
}

# resource "cloudflare_record" "root" {
#   zone_id = cloudflare_zone.zone.id
#
#   name            = local.domain
#   type            = "CNAME"
#   proxied         = false
#   ttl             = 1
#   allow_overwrite = false
#   content         = "quadtask.fly.dev"
# }

# ✗ fly certs add "*.quadtask.app"
#
# You are creating a wildcard certificate for *.quadtask.app
# We are using lets_encrypt for this certificate.
#
# You can direct traffic to *.quadtask.app by:
#
# 1: Adding an A record to your DNS service which reads
#
#     A @ 66.241.125.33
#
# You can validate your ownership of *.quadtask.app by:
#
# 2: Adding an CNAME record to your DNS service which reads:
#     CNAME _acme-challenge.quadtask.app => quadtask.app.6l9egj.flydns.net.

resource "cloudflare_record" "wildcard" {
  zone_id = cloudflare_zone.zone.id

  name            = "_acme-challenge.quadtask.app"
  type            = "CNAME"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = "quadtask.app.6l9egj.flydns.net"
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
# TODO: couldn't add these with terraform for some reason
# resource "cloudflare_record" "mx20" {
#   zone_id = cloudflare_zone.zone.id
#
#   name            = local.domain
#   type            = "MX"
#   proxied         = false
#   ttl             = 1
#   allow_overwrite = false
#   priority        = 20
#
#   data {
#     target   = "mailsec.protonmail.ch"
#     priority = 20
#   }
# }
# resource "cloudflare_record" "mx10" {
#   zone_id = cloudflare_zone.zone.id
#
#   name            = local.domain
#   type            = "MX"
#   proxied         = false
#   ttl             = 1
#   allow_overwrite = false
#   priority        = 10
#
#   data {
#     target = "mail.protonmail.ch"
#     priority = 10
#   }
# }
resource "cloudflare_record" "dmarc" {
  zone_id = cloudflare_zone.zone.id

  name            = "_dmarc"
  type            = "TXT"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = "v=DMARC1; p=quarantine"
}
resource "cloudflare_record" "spf" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "TXT"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = "v=spf1 include:_spf.protonmail.ch ~all"
}
resource "cloudflare_record" "txt_protonmail_verification" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "TXT"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = "protonmail-verification=1ea589b25b1cb7588602576b2aa0b6ba161a384d"
}
# resource "cloudflare_record" "txt_simplelogin_verification" {
#   zone_id = cloudflare_zone.zone.id
#
#   name            = local.domain
#   type            = "TXT"
#   proxied         = false
#   ttl             = 1
#   allow_overwrite = false
#   content         = "sl-verification=esmctdubavptsmklzsewyjzzkxgdrm"
# }
# resource "cloudflare_record" "txt_google_verification" {
#   zone_id = cloudflare_zone.zone.id
#
#   name            = local.domain
#   type            = "TXT"
#   proxied         = false
#   ttl             = 1
#   allow_overwrite = false
#
#   content = "google-site-verification=dMUQLxVo2lv2G5qLUC3oo8rAnwFg5XFHWMKQVKFEDns"
# }

################################################################################

# Create a new Namecheap domain DNS
resource "namecheap_domain_dns" "mydns" {
  provider = namecheapecosystem
  domain   = local.domain

  nameservers = cloudflare_zone.zone.name_servers
}
