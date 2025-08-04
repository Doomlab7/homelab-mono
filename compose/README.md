# Homelab Compose

My repository for the application that I use docker compose stacks to run "in production"

My development stuff is in my [dev repo](https://github.com/pypeaday/homelab-dev)

## Monitoring

My monitoring stack is in my [monitoring repo](https://github.com/Doomlab7/homelab-monitoring)

Glances/Netdata are apart of `monitoring-visibility` which is copied between the 2 hosts due to slightly different docker groups and traefik config

## Networks

`phantomlink` is my homelab network - created with a simple script in ghost/

# Docker Compose Project Overview

This project utilizes Docker Compose to manage multiple stacks of applications, running on a couple hosts. The stacks are organized by host, making it easy to identify and manage individual applications.

## Docker Context

I'm using `.envrc` to set the docker context for whichever machine I'm in


## Listing Applications

To list all the applications in the project, you can use the following command:

# TODO

Each application has its own README file located within the corresponding subfolder. These files provide more detailed information about each application, including setup instructions and configuration options.

## Contributing

If you'd like to contribute to this project, please fork the repository and submit a pull request. We welcome all kinds of contributions, including new features, bug fixes, and documentation updates.

# End of Project README
