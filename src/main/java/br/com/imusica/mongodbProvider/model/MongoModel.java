package br.com.imusica.mongodbProvider.model;

import java.io.Serializable;

public abstract class MongoModel implements Serializable
{
	private static final long serialVersionUID = 3488803719353156993L;
	
	private MongoIdentity _id;
	
	public MongoModel() {}

	public MongoModel(String id) {
		_id = new MongoIdentity(id);
	}

	public MongoIdentity get_id() {
		return _id;
	}
	
	public void setIdentity(String id) {
		_id = new MongoIdentity(id);
	}
	
	public String getIdentity() {
		return _id.get$oid();
	}

	public void set_id(MongoIdentity _id) {
		this._id = _id;
	}
}
