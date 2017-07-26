build_receiver:
		echo "Build message receiver server"
		CGO_ENABLED=0 GOOS=linux go build -o ./publisher/server ./receiver/. 

build_publisher:
	  echo "Build message publisher"
		CGO_ENABLED=0 GOOS=linux go build -o ./publisher/server ./publisher/. 

run: build_receiver build_publisher
	docker-compose up --build
