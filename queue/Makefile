build_read:
		echo "Build Products read server"
		CGO_ENABLED=0 GOOS=linux go build -o ./reader/server ./reader/. 

build_write:
	  echo "Build Products write server"
		CGO_ENABLED=0 GOOS=linux go build -o ./writer/server ./writer/. 

run: build_write build_read
	docker-compose up --build
