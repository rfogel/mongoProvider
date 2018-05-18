package br.com.imusica.mongodbProvider;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoDBProvider
{	
	private MongoClient mongoClient;

	private MongoDatabase mongoDatabase;
	
	private MongoCredential mongoCredentials;
	
	private MongoClientOptions mongoClientOptions;
	
	public MongoDBProvider() {}
	
	public MongoDBProvider(String host, int port, int timeout, int maxIdleTime, int maxWaitTime, int maxConnections, int minConnections, String database)
	{
		mongoClientOptions = MongoClientOptions.builder().connectTimeout(timeout).maxConnectionIdleTime(maxIdleTime).maxWaitTime(maxWaitTime).connectionsPerHost(maxConnections).minConnectionsPerHost(minConnections).build();
	
		ServerAddress serverAddress = new ServerAddress(host, port);
		
		mongoClient = new MongoClient(serverAddress, mongoClientOptions);
		
		mongoDatabase = mongoClient.getDatabase(database);
	}

	public MongoDBProvider(String hosts, String replicaSetName, int timeout, int maxIdleTime, int maxWaitTime, int maxConnections, int minConnections, boolean keepAlive, String database, String userName, String password)
	{
		mongoCredentials = MongoCredential.createCredential(userName, database, password.toCharArray());

		mongoClientOptions = MongoClientOptions.
				builder().
				connectTimeout(timeout).
				maxConnectionIdleTime(maxIdleTime).
				maxWaitTime(maxWaitTime).
				connectionsPerHost(maxConnections).
				socketKeepAlive(keepAlive).
				minConnectionsPerHost(minConnections).
				readPreference(ReadPreference.primary()).
				requiredReplicaSetName(replicaSetName).
				writeConcern(WriteConcern.MAJORITY).
				build();

		List<ServerAddress> serverAddresses = new ArrayList<>();
		String[] hostArray = hosts.split(",");

		for(String host: hostArray) {
			serverAddresses.add(new ServerAddress(host.split(":")[0], Integer.parseInt(host.split(":")[1])));
		}

		mongoClient = new MongoClient(serverAddresses, Collections.singletonList(mongoCredentials), mongoClientOptions);

		mongoDatabase = mongoClient.getDatabase(database);
	}
	
	public MongoDBProvider(String host, int port, int timeout, int maxIdleTime, int maxWaitTime, int maxConnections, int minConnections, String database, String userName, String password) 
	{
		mongoCredentials = MongoCredential.createCredential(userName, database, password.toCharArray());
		
		mongoClientOptions = MongoClientOptions.builder().connectTimeout(timeout).maxConnectionIdleTime(maxIdleTime).maxWaitTime(maxWaitTime).connectionsPerHost(maxConnections).minConnectionsPerHost(minConnections).build();
		
		ServerAddress serverAddress = new ServerAddress(host, port);
		
		mongoClient = new MongoClient(serverAddress, Collections.singletonList(mongoCredentials), mongoClientOptions);
		
		mongoDatabase = mongoClient.getDatabase(database);
	}
	
	public void closeDB() {
		mongoClient.close();
	}
	
	public MongoDatabase getDatabase() {
		return mongoDatabase;
	}
	
	public String getDatabaseName() {
		return mongoDatabase.getName();
	}
	
	public MongoDatabase getDatabase(String database) {
		return mongoClient.getDatabase(database);
	}
	
	public MongoClient getClient() {
		return mongoClient;
	}
	
	public boolean isUpAndRunning() 
	{
		DBObject cmd = new BasicDBObject("ping","1");
		Document runCommand = mongoDatabase.runCommand((Bson) cmd);
		Object okValue = runCommand.get("ok");
		if (okValue instanceof Boolean) {
            return (Boolean) okValue;
        } else if (okValue instanceof Number) {
            return ((Number) okValue).intValue() == 1;
        } else {
            return false;
        }
	}
}
