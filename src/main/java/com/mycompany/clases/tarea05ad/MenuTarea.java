 /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.clases.tarea05ad;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 *
 * @author JoseM
 */
public class MenuTarea {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException {
                Class.forName("org.postgresql.Driver");
                Configuracion configuracion = new Configuracion();
		File archivoConf = new File("configuraciondb.json");
		try {
                    // Creamos un flujo de entrada para el archivo.
                    FileReader flujoDatos;
                    flujoDatos = new FileReader(archivoConf);
                    // Creamos el bufer de entrada.
                    BufferedReader buferEntrada = new BufferedReader(flujoDatos);
                    // Vamos leyendo línea a línea.
                    StringBuilder jsonBuilder = new StringBuilder();
                    String linea;
                    while ((linea = buferEntrada.readLine()) != null) {
                        jsonBuilder.append(linea).append("\n");
                    }
                    // Cerramos el buffer.
                    buferEntrada.close();
                    // Creamos el String para todas las líneas que se han leído.
                    String json = jsonBuilder.toString();
                    // Pasamos al json la clase con la que se corresponde.
                    Gson gson = new Gson();
                    configuracion = gson.fromJson(json, Configuracion.class);
		} 
                catch (FileNotFoundException e) {
                    System.out.println("No se encuentra el archivo.");
		} 
                catch (IOException e) {
                    System.out.println("Error de tipo E/S.");
		}
		/*Sacamos del archivo .json la información de la
                base de datos a la que nos vamos a conectar.*/
		String url = new String(configuracion.getDbconnection().getAddress());
		String db = new String(configuracion.getDbconnection().getName());
		/* Extraemos también del .json
                las propiedades de la conexión.*/
		Properties props = new Properties();
		props.setProperty("user", configuracion.getDbconnection().getUser());
		props.setProperty("password", configuracion.getDbconnection().getPassword());
		// Dirección de conexión a la base de datos.
		String postgres = "jdbc:postgresql://"+url+"/"+db;
		// Conectamos con la base de datos.
		try {
                    Connection conn = DriverManager.getConnection(postgres, props);
                    System.out.println("Conexión con la base de datos realizada con éxito.");
                    System.out.println("Creando las tablas -directorio- y -archivo- si no existen...\n");
                    // Creamos la tabla directorio.
                    String sqlTableCreation = new String(
                    		"CREATE TABLE IF NOT EXISTS directorio (id serial primary key, nombre text not null);");
                    // Ejecutamos la sentencia SQL anterior.
                    CallableStatement createFunction = conn.prepareCall(sqlTableCreation);
                    createFunction.execute();		
                    // Creamos la tabla archivo.
                    String sqlTableCreation1 = new String(
				"CREATE TABLE IF NOT EXISTS archivo (id serial primary key, nombre text not null,idDirectorio integer references directorio(id),binario bytea not null);");
                    // Ejecutamos la sentencia SQL anterior.
                    createFunction = conn.prepareCall(sqlTableCreation1);
                    createFunction.execute();
                    createFunction.close();		
                    File directorio = new File(configuracion.getApp().getDirectory());
                    String raiz=directorio.getParent()+File.separator+directorio.getName();
                    String punto=raiz.replace(raiz,".");
                    crearFuncion(conn);
                    crearTrigger(conn);
                    System.out.println("~Iniciando comparación principal de los ficheros existentes localmente y los de la base de datos.~");
                    recorrerFicheros(directorio, conn, raiz);
                    System.out.println("Comprobación general finalizada.");
                    //Cerramos la conexión con la base de datos.
                    if(conn!=null) conn.close();
		}
                catch (SQLException ex) {
                    System.err.println("¡Error! " + ex.toString());
		}
                try{
                    Connection lConn = DriverManager.getConnection(postgres,props);
                    Connection nConn = DriverManager.getConnection(postgres,props);
                    Listener listener = new Listener(lConn);
                    Notifier notifier = new Notifier(nConn, configuracion);
                    listener.start();
                    notifier.start();
                }
                catch (SQLException ex) {
                    System.err.println("¡Error! " + ex.toString());
		}
    }
    //Método para recorrer los distintos ficheros del directorio raíz.
    public static void recorrerFicheros(File fichero, Connection conn,String raiz) throws FileNotFoundException, SQLException, ClassNotFoundException {
		if (fichero.isFile()) {
                    int idDirectorio = idDirectorio(conn, fichero.getParent(), raiz);
                    String nombreFichero = fichero.getName();
                    if (!(comprobarArchivo(conn, nombreFichero) && comprobarIDDirectorio(conn, idDirectorio))) {
                        FileInputStream fis;
                        System.out.println("\t\t-El archivo \""+ nombreFichero +"\" no existe en la base de datos. Insertando...");
                        try {
                            fis = new FileInputStream(fichero);
                            // Creamos la consulta para insertar en la tabla el archivo.
                            String sqlInsert = new String("INSERT INTO archivo(nombre,idDirectorio,binario) VALUES (?,?,?);");
                            PreparedStatement ps;
                            ps = conn.prepareStatement(sqlInsert);
                            // Añadimos como primer parámetro el nombre del archivo
                            ps.setString(1, fichero.getName());
                            // Añadimos como segundo parámetro el id del directorio del archivo.
                            ps.setInt(2, idDirectorio(conn,fichero.getParent(),raiz));
                            // Añadimos como tercer parámetro el arquivo binario.
                            ps.setBinaryStream(3, fis, (int)fichero.length());
                            // Ejecutamos la consulta.
                            ps.executeUpdate();
                            // Cerramos la consulta y el archivo abierto.
                            ps.close();
                            fis.close();
                        }
                        catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    else{
                        System.out.println("\t\t-El archivo \""+nombreFichero+"\" ya existe en la base de datos.");
                    }
		}
                if (fichero.isDirectory() ) {
                    String path=fichero.getPath();
                    try {
                        String nombre=path.replace(raiz,".");
                        if (!comprobarDirectorio(conn, nombre)) {
                            System.out.println("\t+El directorio \""+fichero+"\" no existe en la base de datos. Insertando...");
                            // Creamos la consulta para insertar en la tabla directorio.
                            String sqlInsert = new String("INSERT INTO directorio(nombre) VALUES (?);");
                            PreparedStatement ps;
                            ps = conn.prepareStatement(sqlInsert);
                            // Añadimos como primer parámetro el nombre del directorio.
                            ps.setString(1, nombre);
                            // Ejecutamos la consulta.
                            ps.executeUpdate();
                            // Cerramos la consulta y el archivo abierto.
                            ps.close();
                        }
                        int idDirectorio = idDirectorio(conn, fichero.getParent(), raiz)+1;
                        comprobarArchivosBD(fichero, conn, idDirectorio, path);
                        
                    }
                    catch (SQLException e) {                        
                        e.printStackTrace();
                    }
                    for (File ficheroHijo : fichero.listFiles()) {
                        recorrerFicheros(ficheroHijo, conn, raiz);
                    }
		}
	}
        //Método para recorrer los distintos ficheros del directorio raíz.
    /*public static void comprobarHilo(File fichero, Connection conn,String raiz) throws FileNotFoundException, SQLException, ClassNotFoundException {
		if (fichero.isFile()) {
                    int idDirectorio = idDirectorio(conn, fichero.getParent(), raiz);
                    String nombreFichero = fichero.getName();
                    if (!(comprobarArchivo(conn, nombreFichero) && comprobarIDDirectorio(conn, idDirectorio))) {
                        FileInputStream fis;
                        System.out.println("\t\t-El archivo \""+ nombreFichero +"\" no existe en la base de datos. Insertando...");
                        try {
                            fis = new FileInputStream(fichero);
                            // Creamos la consulta para insertar en la tabla el archivo.
                            String sqlInsert = new String("INSERT INTO archivo(nombre,idDirectorio,binario) VALUES (?,?,?);");
                            PreparedStatement ps;
                            ps = conn.prepareStatement(sqlInsert);
                            // Añadimos como primer parámetro el nombre del archivo
                            ps.setString(1, fichero.getName());
                            // Añadimos como segundo parámetro el id del directorio del archivo.
                            ps.setInt(2, idDirectorio(conn,fichero.getParent(),raiz));
                            // Añadimos como tercer parámetro el arquivo binario.
                            ps.setBinaryStream(3, fis, (int)fichero.length());
                            // Ejecutamos la consulta.
                            ps.executeUpdate();
                            // Cerramos la consulta y el archivo abierto.
                            ps.close();
                            fis.close();
                        }
                        catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    else{
                        System.out.println("\t\t-El archivo \""+nombreFichero+"\" ya existe en la base de datos.");
                    }
		}
                if (fichero.isDirectory() ) {
                    String path=fichero.getPath();
                    try {
                        String nombre=path.replace(raiz,".");
                        if (!comprobarDirectorio(conn, nombre)) {
                            System.out.println("\t+El directorio \""+fichero+"\" no existe en la base de datos. Insertando...");
                            // Creamos la consulta para insertar en la tabla directorio.
                            String sqlInsert = new String("INSERT INTO directorio(nombre) VALUES (?);");
                            PreparedStatement ps;
                            ps = conn.prepareStatement(sqlInsert);
                            // Añadimos como primer parámetro el nombre del directorio.
                            ps.setString(1, nombre);
                            // Ejecutamos la consulta.
                            ps.executeUpdate();
                            // Cerramos la consulta y el archivo abierto.
                            ps.close();
                        }
                    }
                    catch (SQLException e) {                        
                        e.printStackTrace();
                    }
                    for (File ficheroHijo : fichero.listFiles()) {
                        comprobarHilo(ficheroHijo, conn, raiz);
                    }
		}
	}
        */
        
        //Método para obtener el id de un directorio de la base de datos.
	public static int idDirectorio(Connection conn,String directorio,String raiz) {
		int id=0;
		String nombre=directorio.replace(raiz,".");
		 PreparedStatement stmt;
		try {
                    stmt = conn.prepareStatement("SELECT id FROM directorio WHERE nombre = ?");
                    stmt.setString(1,nombre);
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        id = rs.getInt("id");
                    };
		} 
                catch (SQLException e) {
                    e.printStackTrace();
		}
		return id;
	}
        //Método para comprobar si ya existe el directorio en la base de datos.
        public static boolean comprobarDirectorio(Connection conn, String nombre) {
		boolean existe = false;
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement("SELECT nombre FROM directorio");
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				if (rs.getString("nombre").equals(nombre)) {
                                    if(nombre.equals(".")){
					System.out.println("\t+El directorio raíz ya existe en la base de datos.");
					existe = true;
                                    }
                                    else{
					System.out.println("\t+El directorio \""+nombre+"\" ya existe en la base de datos.");
					existe = true;
                                    }
				}
			}
			rs.close();
			stmt.close();
		} 
                catch (SQLException e) {
			e.printStackTrace();
		}	
		return existe;
	}
        //Método para comprobar si el archivo existe en la base de datos.
	public static boolean comprobarArchivo(Connection conn, String nombre) throws ClassNotFoundException {
		boolean existe = false;
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement("SELECT nombre FROM archivo");
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
                            if (rs.getString("nombre").equals(nombre)) {
				existe = true;
                            }                                
			}
			rs.close();
			stmt.close();
		} 
                catch (SQLException e) {
			e.printStackTrace();
		}
		
		return existe;
	}
        //Método para comprobar el ID del directorio del archivo en la base de datos.
	public static boolean comprobarIDDirectorio(Connection conn, int id) {
		boolean existe = false;
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement("SELECT id FROM archivo");
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				if (rs.getInt("id") == id) {
					existe = true;
				}
			}
			rs.close();
			stmt.close();
		} 
                catch (SQLException e) {
			e.printStackTrace();
		}
		return existe;
	}
        //Método para comprobar si el archivo existe en la base de datos.
	public static boolean comprobarArchivosBD(File fichero, Connection conn, int idDir, String ruta) throws FileNotFoundException {
                boolean existe = false;
                boolean control = false;
                File [] archivos = fichero.listFiles();
		PreparedStatement stmt;
		try {
                    stmt = conn.prepareStatement("SELECT nombre FROM archivo WHERE idDirectorio=?");
                    stmt.setInt(1, idDir);
                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        existe=false;
                        control= false;
                        if(archivos.length>=1){
                            for (int i=0; i < archivos.length; i++) {
                                File archivo = archivos[i];
                                if (rs.getString("nombre").equals(archivo.getName())) {
                                    existe = true;
                                    control = true;
                                }
                                if(existe==true){
                                    i = archivos.length;
                                }
                            }
                            if(control==false){
                                String nombreArchivo = rs.getString("nombre");
                                System.out.println("\t\t-Falta el archivo -"+nombreArchivo+"-. Descargando de la base de datos...");
                                try {
                                    String sqlGet = new String(
                                            "SELECT binario FROM archivo WHERE nombre = ?;");
                                    PreparedStatement ps2 = conn.prepareStatement(sqlGet);
                                    //Añadimos a la consulta el nombre del archivo que queremos recuperar.
                                    ps2.setString(1, nombreArchivo);
                                    //Ejecutamos la consulta.
                                    ResultSet rs2 = ps2.executeQuery();
                                    //Vamos recuperando todos los bytes de los archivos.
                                    byte[] archivoBytes = null;
                                    while (rs2.next()){ 
                                        archivoBytes = rs2.getBytes(1); 
                                    }
                                    //Cerramos a consulta.
                                    rs2.close(); 
                                    ps2.close();
                                    //Creamos el flujo de datos para guardar el archivo recuperado.
                                    String ficheroSalida = new String(ruta+"\\"+nombreArchivo);//hay que añadir la ruta del archivo
                                    File fileOut = new File(ficheroSalida);
                                    FileOutputStream flujoDatos = new FileOutputStream(fileOut);
                                    //Guardamos el archivo recuperado.
                                    if(archivoBytes != null){
                                        flujoDatos.write(archivoBytes);
                                    }
                                    //Cerramos el flujo de datos de salida.
                                    flujoDatos.close();
                                    existe = false;
                                }
                                catch (FileNotFoundException e) {
                                    e.printStackTrace();
                                }
                                catch (SQLException e) {
                                    e.printStackTrace();
                                }
                                catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }      
                        }
                        else{
                            String nombreArchivo = rs.getString("nombre");
                            System.out.println("\t-Falta el archivo -"+nombreArchivo+"-. Descargando de la base de datos...");
                            try {
                                String sqlGet = new String(
                                            "SELECT binario FROM archivo WHERE nombre = ?;");
                                    PreparedStatement ps2 = conn.prepareStatement(sqlGet);
                                    //Añadimos a la consulta el nombre del archivo que queremos recuperar.
                                    ps2.setString(1, nombreArchivo);
                                    //Ejecutamos la consulta.
                                    ResultSet rs2 = ps2.executeQuery();
                                    //Vamos recuperando todos los bytes de los archivos.
                                    byte[] archivoBytes = null;
                                    while (rs2.next()){ 
                                        archivoBytes = rs2.getBytes(1); 
                                    }
                                    //Cerramos a consulta.
                                    rs2.close(); 
                                    ps2.close();
                                    //Creamos el flujo de datos para guardar el archivo recuperado.
                                    String ficheroSalida = new String(ruta+"\\"+nombreArchivo);//hay que añadir la ruta del archivo
                                    File fileOut = new File(ficheroSalida);
                                    FileOutputStream flujoDatos = new FileOutputStream(fileOut);
                                    //Guardamos el archivo recuperado.
                                    if(archivoBytes != null){
                                        flujoDatos.write(archivoBytes);
                                    }
                                    //Cerramos el flujo de datos de salida.
                                    flujoDatos.close();
                                    existe = false;
                            }
                            catch (FileNotFoundException e) {
                                e.printStackTrace();
                            }
                            catch (SQLException e) {
                                e.printStackTrace();
                            }
                            catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    rs.close();
                    stmt.close();
		} 
                catch (SQLException e) {
			e.printStackTrace();
		}
		
		return existe;
	}
       
        
        //Método con la sentencia SQL para crear una función.
        public static void crearFuncion(Connection conn) {
		String sqlCreateFunction = new String("CREATE OR REPLACE FUNCTION notificar_archivo() "+
                                                        "RETURNS trigger AS $$ "+
                                                        "BEGIN " +
                                                        "PERFORM pg_notify('nuevoarchivo',NEW.id::text); "+
                                                        "RETURN NEW; "+
                                                        "END; "+
                                                        "$$ LANGUAGE plpgsql; ");
		// Executamos a sentencia SQL anterior
		CallableStatement createFunction;
		try {
			createFunction = conn.prepareCall(sqlCreateFunction);
			createFunction.execute();
			createFunction.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
        //Trigger
        public static void crearTrigger(Connection conn) {
		String sqlCreateTrigger = new String(
                "DROP TRIGGER IF EXISTS not_nuevo_archivo ON archivo; "+
                "CREATE TRIGGER not_nuevo_archivo AFTER INSERT ON archivo FOR EACH ROW "+
                "EXECUTE PROCEDURE notificar_archivo(); ");
		try {
                    CallableStatement createTrigger = conn.prepareCall(sqlCreateTrigger);
                    createTrigger.execute();
                    createTrigger.close();
                    
		} 
                catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
