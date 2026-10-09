docker compose up -d postgres
- if it say port alr allocated, 
-  docker ps --format "table {{.Names}}\t{{.Ports}}" , do this to see which project using
- docker stop <container-name> , and stop it using this
- docker compose up -d postgres , then start again
