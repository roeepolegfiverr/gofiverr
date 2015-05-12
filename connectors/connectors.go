package shared

import (
	"database/sql"
	"fmt"
	"github.com/fzzy/radix/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"go_live/shared/errors"
	"gopkg.in/mgo.v2"
)

type clients struct {
	redisClients   map[string]*redis.Client
	mongoClients   map[string]*mgo.Session
	mySqlClients   map[string]*sql.DB
	rabbitConsumer *amqp.Channel
}

var (
	//Clients contain all the different connections to various dbs.
	Clients                      *clients
	RedisPipelineQueueEmptyError error = redis.PipelineQueueEmptyError
)

// Initialize the connectors
func InitConnectors(initRabbit bool) {
	Clients = &clients{
		redisClients:   make(map[string]*redis.Client),
		mongoClients:   make(map[string]*mgo.Session),
		mySqlClients:   make(map[string]*sql.DB),
		rabbitConsumer: nil,
	}

	// Only init Rabbit for workers
	if initRabbit {
		Clients.createRabbit()
	}
}

// ProperShutdown handle proper cleanup
func (clients *clients) ProperShutdown() {

	// Close all mysql connections
	for _, sqlDb := range clients.mySqlClients {
		sqlDb.Close()
	}

	//Close Redis
	for _, redisDb := range clients.redisClients {
		redisDb.Close()
	}

	//Close Mongo
	for _, mongoDb := range clients.mySqlClients {
		mongoDb.Close()
	}

	if clients.rabbitConsumer != nil {
		clients.rabbitConsumer.Close()
	}
}

func (clients *clients) Rabbit() (channel *amqp.Channel) {

	if Clients.rabbitConsumer != nil {
		return Clients.rabbitConsumer
	}
	conn, err := amqp.Dial(Config.GetNamedAmqp("rabbit"))
	if err != nil {
		fmt.Printf("error connecting rabbit %s", err)
		ErrorLog(errors.Wrap(err, err.Error()))
		panic(err)
	}

	channel, err = conn.Channel()
	if err != nil {
		fmt.Printf("error connecting channel rabbit %s", err)
		ErrorLog(errors.Wrap(err, err.Error()))
		panic(err)
	}
	Clients.rabbitConsumer = channel
	return
}

// Get the default MySql client
func (clients *clients) MySql() (client *sql.DB, err error) {
	return clients.NamedMySql("default")
}

// Get named MySql connection
func (clients *clients) NamedMySql(clientName string) (client *sql.DB, err error) {
	if clientName == "" {
		clientName = "default"
	}
	if client, ok := clients.mySqlClients[clientName]; ok {
		return client, nil
	}

	return clients.createMySqlClient(clientName)
}

// Get the default Redis client
func (clients *clients) Redis() (client *redis.Client, err error) {
	return clients.NamedRedis("default")

}

// Get named Redis connection
func (clients *clients) NamedRedis(clientName string) (client *redis.Client, err error) {
	if clientName == "" {
		clientName = "default"
	}
	if client, ok := clients.redisClients[clientName]; ok {
		return client, nil
	}

	return clients.createRedisClient(clientName)
}

// Get the default Mongo client
func (clients *clients) Mongo(clientName string) (client *mgo.Session, err error) {
	return clients.NamedMongo("default")

}

// Get named Mongo connection
func (clients *clients) NamedMongo(clientName string) (client *mgo.Session, err error) {
	if clientName == "" {
		clientName = "default"
	}
	if client, ok := clients.mongoClients[clientName]; ok {
		// always return a copy of a session.
		return client.Copy(), nil
	}

	client, err = clients.createMongoClient(clientName)

	// always return a copy of a session.
	return client.Copy(), err
}

func (clients *clients) createMySqlClient(clientName string) (client *sql.DB, err error) {
	user := Config.Get(fmt.Sprintf("mysql.%s.user", clientName), "root")
	password := Config.Get(fmt.Sprintf("mysql.%s.password", clientName), "")
	host := Config.Get(fmt.Sprintf("mysql.%s.host", clientName), "localhost")
	port := Config.GetInt(fmt.Sprintf("mysql.%s.port", clientName), 3306)
	dbName := Config.Get(fmt.Sprintf("mysql.%s.database", clientName), "fiverr_dev")

	fmt.Println(fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbName))
	client, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbName))
	if err != nil {
		ErrorLog(errors.Wrap(err, err.Error()))
		return nil, err
	}

	// Open doesn't open a connection. Validate DSN data:
	err = client.Ping()
	if err != nil {
		ErrorLog(errors.Wrap(err, err.Error()))
		return nil, err
	}

	client.SetMaxOpenConns(10) //TODO: Ask Marina
	clients.mySqlClients[clientName] = client
	return client, nil

}

func (clients *clients) createRedisClient(clientName string) (client *redis.Client, err error) {
	host := Config.Get(fmt.Sprintf("redis.%s.host", clientName), "localhost")
	port := Config.GetInt(fmt.Sprintf("redis.%s.port", clientName), 6379)

	client, err = redis.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		ErrorLog(errors.Wrap(err, err.Error()))
		return nil, err
	}

	clients.redisClients[clientName] = client
	return client, nil
}

func (clients *clients) createMongoClient(clientName string) (client *mgo.Session, err error) {
	host := Config.Get(fmt.Sprintf("mongo.%s.host", clientName), "localhost")
	port := Config.GetInt(fmt.Sprintf("mongo.%s.port", clientName), 27017)

	client, err = mgo.Dial(fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		ErrorLog(errors.Wrap(err, err.Error()))
		return nil, err
	}

	clients.mongoClients[clientName] = client
	return client, nil
}

func (clients *clients) createRabbit() (channel *amqp.Channel, err error) {
	conn, err := amqp.Dial(Config.GetNamedAmqp("rabbit"))
	if err != nil {
		ErrorLog(errors.Wrap(err, err.Error()))
		return nil, err
	}

	channel, err = conn.Channel()
	if err != nil {
		ErrorLog(errors.Wrap(err, err.Error()))
		return nil, err
	}

	clients.rabbitConsumer = channel
	return channel, nil
}
