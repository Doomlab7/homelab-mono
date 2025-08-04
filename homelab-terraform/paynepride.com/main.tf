resource "cloudflare_zone" "zone" {
  # account_id = data.cloudflare_accounts.cloudflare_account_data.id
  account_id = local.account_id
  zone       = local.domain
}

resource "cloudflare_record" "wildcard" {
  zone_id = cloudflare_zone.zone.id

  name            = "*"
  type            = "A"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = var.wan
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

  content = "v=spf1 include:_spf.protonmail.ch mx ~all"
}
resource "cloudflare_record" "txt_protonmail_verification" {
  zone_id = cloudflare_zone.zone.id

  name            = local.domain
  type            = "TXT"
  proxied         = false
  ttl             = 1
  allow_overwrite = false

  content = "protonmail-verification=1f9b5a5e1dd86d38df4600d3ea093e97442607a2"
}

# the acme challenge ones I think are from LE

################################################################################

# Create a new Namecheap domain DNS
resource "namecheap_domain_dns" "mydns" {
  provider = namecheapecosystem
  domain   = local.domain

  nameservers = cloudflare_zone.zone.name_servers
}
