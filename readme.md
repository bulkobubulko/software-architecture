# Homework 3

## Microservices architecture
![microservices-architecture-0](data/microservices-architecture-0.png)
![microservices-architecture-1](data/microservices-architecture-1.png)

For this task we update architecture as such:
![microservices-architecture-2](data/microservices-architecture-2.png)

## Architecture consists of three microservices:

- facade-service – accepts POST/GET requests from the client
- logging-service – stores all incoming messages in memory and can return them
- messages-service – currently acts as a placeholder, returning a static message when accessed

## Build and run the services
```
docker-compose down --remove-orphans && docker-compose up --build -d
```

Send 10 messages through the facade service
```
for i in {1..10}; do
  curl -X POST "http://localhost:8000/" -H "Content-Type: application/json" -d "{\"msg\": \"msg$i\"}"
  echo ""
done
```

![img-0](data/img-0.png)

Check the logs to see which messages were received by each service
```
docker-compose logs logging-service-1
docker-compose logs logging-service-2
docker-compose logs logging-service-3
```

![img-1](data/img-1.png)


Get all messages
```
curl -X GET "http://localhost:8000/"
```

![img-2](data/img-2.png)

Stop logging service
```
docker-compose stop logging-service-1
```

![img-3](data/img-3.png)

Get all messages after stopping loggin service
```
curl -X GET "http://localhost:8000/"
```

![img-4](data/img-4.png)

Even after stopping one of loggin services we still have all the messages!

