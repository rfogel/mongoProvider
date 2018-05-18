package br.com.imusica.mongodbProvider.exception;

public class MongoDBException extends Exception
{
	private static final long serialVersionUID = -8714979981066954822L;

	public MongoDBException(String exception) {
		super(exception);
	}
	
	public MongoDBException(Exception exception) {
		super(exception);
	}
}
