.PHONY: run
run:
	@echo "Starting echo server in docker..."
	docker build -t echo-server:latest .
	docker run -it --rm -p 7777:7777 --name echo-server echo-server:latest

get-health:
	curl -v localhost:7777/health

post-msg:
	curl -X POST localhost:7777 -d @./data/post.json