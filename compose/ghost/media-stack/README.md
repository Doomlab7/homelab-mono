# Media Stack

A fairly big stack that provisions several media server components

- Jellyfin
- Jellystat
- Jellyseerr (in the *arrs stack)
- Booksonic
- Airsonic
- Audiobookshelf
- Ubooquity
- Pinchflat


## Pinchflat

# TODO:
- take down pinchflat stack
- take down media-stack
- move media-stack data to their own zfs dataset 
- move pinchflat data to its own zfs dataset
- setup relevant backup of that dataset in zfs-ops
- stand up media-stack
- move vocal remover pinchflat volume mount

A new YouTube downloader - it's not meant to consuming the media, but more for archiving.

I plan to have Jellyfin pull this in someday

[github](https://github.com/kieraneglin/pinchflat)

Note that the vocal remover watcher watches the pinchflat download directory
