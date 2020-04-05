package com.mycompany.clases.tarea05ad;


/**
 *
 * @author JoseM
 */
public class Configuracion {
	
	DbConnection dbConnection;
	App app;
	
	
	
	public Configuracion() {
		super();
		dbConnection=new DbConnection();
		app=new App();
		
		// TODO Auto-generated constructor stub
	}
	public Configuracion(DbConnection dbconnection, App app) {
		super();
		this.dbConnection = dbconnection;
		this.app = app;
	}
	public DbConnection getDbconnection() {
		return dbConnection;
	}
	public void setDbconnection(DbConnection dbconnection) {
		this.dbConnection = dbconnection;
	}
	public App getApp() {
		return app;
	}
	public void setApp(App app) {
		this.app = app;
	}
	
	

}
