map $http_upgrade $connection_upgrade {
  default upgrade;
  '' close;
}

client_max_body_size 3000M;

server {
  listen 8080 default_server;

  add_header X-Frame-Options "DENY";
  add_header Content-Security-Policy "frame-ancestors 'none';";

  # Location to handle /v2 and rewrite to /api/v2 without sending '/api' to the backend
  location /v2/ {
    client_max_body_size 3000M;
    proxy_read_timeout 600s;
    proxy_request_buffering off;
    rewrite ^/v2/(.*)$ /v2/$1 break;
    proxy_pass http://server:3000;
    proxy_redirect off;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  }

  location /api/ {
    client_max_body_size 3000M;
    proxy_read_timeout 600s;
    proxy_request_buffering off;
    proxy_pass http://server:3000/;
    proxy_redirect off;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  }

  # Handle Hasura GraphQL
  location /hasura {
    proxy_pass http://hasura:8080/v1/graphql;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "Upgrade";
  }

  # Default location for frontend
  location / {
    proxy_pass http://client:8080/;
    proxy_redirect off;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  }
}
