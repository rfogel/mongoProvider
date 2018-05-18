package br.com.imusica.mongodbProvider.test;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Arrays.asList;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;

import br.com.imusica.mongodbProvider.MongoDBProvider;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.UpdateOptions;

public class MongoTest
{
	public static void main(String[] args) throws ParseException 
	{
		System.setProperty("DEBUG.MONGO", "false");
		System.setProperty("DB.TRACE", "false");
		
		Logger.getLogger("org.mongodb.driver.connection").setLevel(Level.OFF);
		Logger.getLogger("org.mongodb.driver.management").setLevel(Level.OFF);
		Logger.getLogger("org.mongodb.driver.cluster").setLevel(Level.OFF);
		Logger.getLogger("org.mongodb.driver.protocol.insert").setLevel(Level.OFF);
		Logger.getLogger("org.mongodb.driver.protocol.query").setLevel(Level.OFF);
		Logger.getLogger("org.mongodb.driver.protocol.update").setLevel(Level.OFF);
		
		// insertUnique();
		// insertOrUpdate();
		//drop();
		//getAllDevice();
		//getAllDeviceInfo();
		//getAllCampaign();
		// getOne();
		
		//insert4();
		questionCounter();
	}
	
	public static void test1() throws ParseException {
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
		db.getCollection("restaurants").insertOne(
				new Document("address", new Document().append("street", "2 Avenue").append("zipcode", "10075").append("building", "1480")
						.append("coord", asList(-73.9557413, 40.7720266)))
						.append("borough", "Manhattan")
						.append("cuisine", "Italian")
						.append("grades",
								asList(new Document().append("date", format.parse("2014-10-01T00:00:00Z")).append("grade", "A").append("score", 11),
										new Document().append("date", format.parse("2014-01-16T00:00:00Z")).append("grade", "B").append("score", 17))).append("name", "Vella")
						.append("restaurant_id", "41704620"));
		
		FindIterable<Document> iterable = db.getCollection("device").find();
		
		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document);
			}
		});
		
		mongoClient.close();
	}
	
	public static void getAllDevice() throws ParseException {
		System.out.println("-----getAllDevice-----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("device");
		
		FindIterable<Document> iterable = collection.find();
		
		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document);
			}
		});
		
		mongoClient.close();
	}
	
	public static void getAllDeviceInfo() throws ParseException {
		System.out.println("-----getAllDeviceInfo-----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("deviceInfo");
		
		FindIterable<Document> iterable = collection.find();
		
		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document);
			}
		});
		
		mongoClient.close();
	}
	
	public static void getAllCampaign() throws ParseException {
		System.out.println("-----getAllCampaign-----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("campaign");
		
		FindIterable<Document> iterable = collection.find();
		
		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document);
			}
		});
		
		mongoClient.close();
	}
	
	public static void getOne() throws ParseException {
		System.out.println("----getOne-----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("deviceInfo");
		
		Document doc = new Document();
		doc.append("info", "gcmId");
		
		FindIterable<Document> iterable = collection.find(doc);
		
		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document);
			}
		});
		
		mongoClient.close();
	}
	
	public static void insert() throws ParseException {
		System.out.println("----insert----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("deviceInfo");
		
		if (collection.find(eq("info", "gcmId")).first() == null)
			collection.insertOne(new Document().append("info", "gcmId"));
		if (collection.find(eq("info", "platform")).first() == null)
			collection.insertOne(new Document().append("info", "platform"));
		
		mongoClient.close();
	}
	
	public static void insert2() throws ParseException {
		System.out.println("----insert 2----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("pushParameter");

		//collection.insertOne(new Document().append("_id", "utm_source"));
		//collection.insertOne(new Document().append("_id", "utm_medium"));
		collection.insertOne(new Document().append("_id", "utm_term"));
		collection.insertOne(new Document().append("_id", "utm_content"));
		collection.insertOne(new Document().append("_id", "utm_campaign"));
		
		mongoClient.close();
	}
	
	public static void bemobiCounter() throws ParseException {
		System.out.println("----insert 3----");
		
		//MongoClient mongoClient = new MongoClient();
		MongoDBProvider mongoClient = new MongoDBProvider("mongorbt.int.imusicacorp.com", 27017, 100, 100, 100, 100, 10, "rbt", "rbt", "J39ip1ty3czDD9W5SFXT");
		
		MongoDatabase db = mongoClient.getDatabase("rbt");
		
		MongoCollection<Document> collection = db.getCollection("bemobiTransactionCounter");

		collection.insertOne(new Document().append("_id", "counter").append("seq", new Long("0")));
		
		mongoClient.closeDB();
	}
	
	public static void questionCounter() throws ParseException {
		System.out.println("----insert 3----");
		
		MongoDBProvider mongoClient = new MongoDBProvider("localhost", 27017, 100, 100, 100, 100, 10, "jm", "jm", "jm");
		
		MongoDatabase db = mongoClient.getDatabase("jm");
		
		MongoCollection<Document> collection = db.getCollection("questionCounter");

		collection.insertOne(new Document().append("_id", "counter").append("seq", new Long("0")));
		
		mongoClient.closeDB();
	}
	
	public static void insert4() throws ParseException {
		System.out.println("----insert 3----");
		
		//MongoClient mongoClient = new MongoClient();
		MongoDBProvider mongoClient = new MongoDBProvider("localhost", 27017, 100, 100, 100, 100, 10, "jm", "jm", "jm");
		
		MongoDatabase db = mongoClient.getDatabase("jm");
		
		MongoCollection<Document> collection = db.getCollection("user");

		collection.insertOne(new Document().append("_id", "525510103372").append("name", "rafael"));
		
		mongoClient.closeDB();
	}
	
	public static void insert5() throws ParseException {
		System.out.println("----insert 4----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("bemobiTransaction");

		collection.insertOne(new Document().append("_id", getNextSequence()));
		
		mongoClient.close();
	}
	
	public static Long getNextSequence() throws ParseException {
		System.out.println("----getNextSequence----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("bemobiTransactionCounter");
		
		Document findOneAndUpdate = collection.findOneAndUpdate(eq("_id","counter"), new Document("$inc", new Document("seq",1)));

		mongoClient.close();
		
		return new Long(String.valueOf(findOneAndUpdate.get("seq")));
	}
	
	public static void insertUnique() throws ParseException {
		System.out.println("----insertUnique----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("deviceInfo");
		
		InsertOneOptions op = new InsertOneOptions();
		op.bypassDocumentValidation(true);
		
		collection.insertOne(new Document().append("_id", "1").append("info", "gcmId"), op);
		collection.insertOne(new Document().append("_id", "2").append("info", "gcmId").append("info", "platform"), op);
		
		mongoClient.close();
	}
	
	public static void insertOrUpdate() {
		System.out.println("----insertOrUpdate----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("deviceInfo");
		
		Document append = new Document().append("_id", "3").append("info", "gcmId");
		
		collection.replaceOne(eq("_id", "3"), append, (new UpdateOptions()).upsert(true));
		
		mongoClient.close();
	}
	
	public static void update() throws ParseException {
		System.out.println("----update----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("deviceInfo");
		
		collection.updateOne(eq("info", "osVersion"), new Document("$set", new Document("info", "osVersion")));
		
		mongoClient.close();
	}
	
	public static void drop() throws ParseException {
		System.out.println("----drop----");
		
		MongoClient mongoClient = new MongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("teste");
		
		MongoCollection<Document> collection = db.getCollection("device");
		
		collection.drop();
		
		collection = db.getCollection("deviceInfo");
		
		collection.drop();
		
		collection = db.getCollection("campaign");
		
		collection.drop();
		
		mongoClient.close();
	}
}
