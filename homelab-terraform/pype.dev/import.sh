# #!/bin/bash
# # import -compact-warnings the cloudflare_pages_domain resource
terraform import -compact-warnings -var-file .tf.vars cloudflare_pages_domain.cf_domain 1f39a816ca049c6f8b6a971d0e56c371/pype-dev/pype.dev

# # # import -compact-warnings the cloudflare_pages_project resource
terraform import -compact-warnings -var-file .tf.vars resource.cloudflare_pages_project.build_config 1f39a816ca049c6f8b6a971d0e56c371/pype-dev

# # # import -compact-warnings the cloudflare_record resources
terraform import -compact-warnings -var-file .tf.vars module.cloudflare_pages.cloudflare_record.cf_domain_record 535583ef8f16f356398483a9414bc852/9b45e437a021248caa443798bb972382

terraform import -compact-warnings -var-file .tf.vars cloudflare_record.protonmail2 535583ef8f16f356398483a9414bc852/52cbf31b2b7d3c067ef50cbda4d25c06
terraform import -compact-warnings -var-file .tf.vars cloudflare_record.protonmail3 535583ef8f16f356398483a9414bc852/85e169f21050fec5a24cf9125f89ac4d
terraform import -compact-warnings -var-file .tf.vars cloudflare_record.protonmail 535583ef8f16f356398483a9414bc852/dd9ffaa4910234d7f851376e1084f7dc

terraform import -compact-warnings -var-file .tf.vars cloudflare_record.root 535583ef8f16f356398483a9414bc852/03190b7d2dda630ef154647b665a4e69
terraform import -compact-warnings -var-file .tf.vars cloudflare_record.www 535583ef8f16f356398483a9414bc852/117f10ac5f8a73898f2ce6f33cf69b9b
terraform import -compact-warnings -var-file .tf.vars cloudflare_record.mx20 535583ef8f16f356398483a9414bc852/ac0fe636c5cbe91a3cf320c90a14cdcf
terraform import -compact-warnings -var-file .tf.vars cloudflare_record.mx10 535583ef8f16f356398483a9414bc852/c61267e7a34300239bf0b8178231cc95
terraform import -compact-warnings -var-file .tf.vars cloudflare_record.dmarc 535583ef8f16f356398483a9414bc852/d33a0eddafd3405c4ab51e6af6434b73

terraform import -compact-warnings -var-file .tf.vars cloudflare_record.spf 535583ef8f16f356398483a9414bc852/fb1f13a97f74127a99a3a2d807b0173c
terraform import -compact-warnings -var-file .tf.vars cloudflare_record.txt_protonmail_verification 535583ef8f16f356398483a9414bc852/bf18aad4915d4f7dafb1df832d7dffc4

terraform import -compact-warnings -var-file .tf.vars cloudflare_record.txt_simplelogin_verification 535583ef8f16f356398483a9414bc852/79805541d6c126557ec0d131c2228520

terraform import -compact-warnings -var-file .tf.vars cloudflare_record.txt_google_verification 535583ef8f16f356398483a9414bc852/69341c19f755ac776fb2287853ff84e5
# terraform import -compact-warnings -var-file .tf.vars module.cloudflare_pages.cloudflare_record.cf_domain_record_develop 535583ef8f16f356398483a9414bc852/22d064db95347c25143b3f32074464b0
# terraform import -compact-warnings -var-file .tf.vars cloudflare_record.cf_domain_www 535583ef8f16f356398483a9414bc852/c0bffd296e12f31b23c261b0626d1119
# # 
# # import -compact-warnings the cloudflare_zone resource
terraform import -compact-warnings -var-file .tf.vars resource.cloudflare_zone.zone 535583ef8f16f356398483a9414bc852

# # import -compact-warnings the namecheap_domain_dns resource
terraform import -compact-warnings -var-file .tf.vars namecheap_domain_dns.mydns pype.dev