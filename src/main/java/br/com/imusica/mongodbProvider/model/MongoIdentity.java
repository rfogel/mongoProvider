package br.com.imusica.mongodbProvider.model;

import java.io.Serializable;

public class MongoIdentity implements Serializable
{
	private static final long serialVersionUID = -9943845326329569L;
	
	public MongoIdentity() {}

	public MongoIdentity(String $oid) {
		super();
		this.$oid = $oid;
	}

	private String $oid;
	
	public String get$oid() {
		return $oid;
	}
	
	public void set$oid(String $oid) {
		this.$oid = $oid;
	}
}
