# MongoDB Replica Set Controller

This is utility for controll [MongoDB replica set](https://docs.mongodb.com/manual/replication/).

## Usage

### Initiate

This command initiate a replica set on all hosts resolved by this three names:
```
$ mongo_rs_controller.py mongo1 mongo2 mongo3
```
Each hostname can resolve with many IPs with mongo.

### Watching changes

```
$ mongo_rs_controller.py --watch mongo1 mongo2 mongo3
```

### Docker Swarm

Create replica set on four replicas of mongo and start watcher:

```
$ docker network create --driver=overlay mongo
$ docker service create --network=mongo --name=mongo --replicas=4 mongo mongod --replSet=rs
$ docker service create --network=mongo --name=mongo-rs-controller --replicas=1 zzzsochi/mongo-rs-controller --watch tasks.mongo
```
You can add or remove replicas and controller reconfigure replica set automatically.
