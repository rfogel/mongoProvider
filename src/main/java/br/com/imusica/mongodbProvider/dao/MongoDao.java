package br.com.imusica.mongodbProvider.dao;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import br.com.imusica.mongodbProvider.MongoDBProvider;
import br.com.imusica.mongodbProvider.exception.MongoDBException;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.UpdateOptions;

public abstract class MongoDao
{	
	protected final static String IDENTITY = "_id";
	
	public abstract MongoDBProvider getMongodbProvider();
	
	public MongoDao() {}
	
	public void insertOne(Document document) {
		getMongodbProvider().getDatabase().getCollection(getCollection()).insertOne(document);
	}
	
	public void insertMany(List<Document> documents) {
		getMongodbProvider().getDatabase().getCollection(getCollection()).insertMany(documents);
	}
	
	public void insertOrUpdate(Document document) {		
		getMongodbProvider().getDatabase().getCollection(getCollection()).replaceOne(eq(MongoDao.IDENTITY,document.get(MongoDao.IDENTITY)),document,(new UpdateOptions()).upsert(true));
	}
	
	public void drop() {
		getMongodbProvider().getDatabase().getCollection(getCollection()).drop();
	}
	
	public Document findOne(Document documentFilter) {		
		return getMongodbProvider().getDatabase().getCollection(getCollection()).find(documentFilter).first();
	}
	
	public Document findOneById(ObjectId id) {		
		return getMongodbProvider().getDatabase().getCollection(getCollection()).find(new Document(IDENTITY, id)).first();
	}
	
	public void deleteOne(Document document) {
		getMongodbProvider().getDatabase().getCollection(getCollection()).deleteOne(document);
	}
	
	public void deleteById(String id) {
		getMongodbProvider().getDatabase().getCollection(getCollection()).deleteOne(new Document(IDENTITY,new ObjectId(id)));
	}
	
	public long count() {
		return getMongodbProvider().getDatabase().getCollection(getCollection()).count();
	}
	
	public long count(Bson filter) {
		return getMongodbProvider().getDatabase().getCollection(getCollection()).count(filter);
	}
	
	public List<Document> findAll(Bson filter) 
	{		
		FindIterable<Document> iterable = getMongodbProvider().getDatabase().getCollection(getCollection()).find(filter);
		
		final List<Document> documents = new ArrayList<>();
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		    	documents.add(document);
		    }
		});
		
		return documents;
	}
	
	public List<Document> findAll(List<Bson> filters) 
	{		
		FindIterable<Document> iterable = getMongodbProvider().getDatabase().getCollection(getCollection()).find(and(filters));
		
		final List<Document> documents = new ArrayList<>();
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		    	documents.add(document);
		    }
		});
		
		return documents;
	}
	
	public List<Document> findAll() 
	{		
		FindIterable<Document> iterable = getMongodbProvider().getDatabase().getCollection(getCollection()).find();
		
		final List<Document> documents = new ArrayList<>();
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		    	documents.add(document);
		    }
		});
		
		return documents;
	}
	
	public Document createDocument(String json) throws MongoDBException
	{
		try {
			return Document.parse(json);		
		} catch (SecurityException | IllegalArgumentException e) {
			throw new MongoDBException(e);
		}	
	}
	
	public abstract String getCollection();
}
