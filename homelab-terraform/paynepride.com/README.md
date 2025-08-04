# paynepride.com

Here I manage my cloudflare and namecheap infrastructure for `paynepride.com`

Steps:

work locally with import script

Need zone ID (zone page on cloudflare)
Need dns record ids (make a change then check audit log in cloudflare account to get the id for the import)


Note that the state of a stack isn't really that important other than for the sake up making updates easily. The point is that if it goes away then one `terraform apply` should bring it all back