N ?= 3

network:
	docker network create --attachable --driver=overlay mongo

service:
	docker service create --network=mongo --name=mongo --replicas=$(N) mongo mongod --replSet=rs

replicas:
	docker service update --replicas=$(N) mongo

clean:
	docker service rm mongo
	docker network rm mongo
