# Homelab

## Compose

I will migrate homelab-compose into this subdirectory

## DataOps

My ZFS related things are in zfs-ops for now

here is restic stuff for now

will bring in any rsync stuff too

## Oliver Sermon Notifier

Requires `gotify` cli

Easy way to install the binary

```bash
nic in homelab-mono   main   ×1  (dev) 󰒄 󱔎 NO PYTHON ENVIORNMENT SET 
⬢ [devbox] ❯ curl -s https://i.paynepride.com/gotify/cli\?as\=gotify | bash
Downloading gotify/cli latest as gotify (linux/amd64).....
######################################################################## 100.0%
Downloaded to /home/nic/projects/personal/homelab-mono/gotify

nic in homelab-mono   main   ×2  (dev) 󰒄 󱔎 NO PYTHON ENVIORNMENT SET 
⬢ [devbox] ❯ sudo mv gotify /usr/local/bin/   
```

## Why A Monorepo?

I have tried organizing various aspects of my life in all kinds of different
ways. My homelab is sprawled out quite far (to me) in terms of how I do
secrets, where I keep config, how I deploy, etc... I and I got tired of coming
up with names for repos (should I make a new repo for temporal? should I have
homelab-platform for temporal and other workflow orchestrators?). I decided to
lean into many repos with homelab- prefix for easy navigation at home, and I'm
using a GitHub organization to share secrets with all those repos... but
overall most of my things could just be in on giant repo and that would at
least handle the question for me about what to name something (at least in
part).

So we're riding the monorepo train for this for a while - all my homelab stuff needs to eventually land here.

Will probably have something similar for Digital Harbor eventually... but then why have 2 GitHub orgs?
