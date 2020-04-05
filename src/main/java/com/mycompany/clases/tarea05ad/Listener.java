/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.clases.tarea05ad;

import java.sql.*;

/**
 *
 * @author JoseM
 */
public class Listener extends Thread{
    private Connection conn;
    private org.postgresql.PGConnection pgconn;

    Listener(Connection conn) throws SQLException{
        this.conn = conn;
        this.pgconn = conn.unwrap(org.postgresql.PGConnection.class);
        Statement stmt = conn.createStatement();
        stmt.execute("LISTEN nuevoarchivo");
        stmt.close();
    }

    public void run(){
        try
        {
            while (true)
            {
                org.postgresql.PGNotification notifications[] = pgconn.getNotifications();

                // If this thread is the only one that uses the connection, a timeout can be used to 
                // receive notifications immediately:
                // org.postgresql.PGNotification notifications[] = pgconn.getNotifications(10000);

                if (notifications != null)
                {
                    for (int i=0; i < notifications.length; i++){
                    System.out.println("¡Añadidos y/o descargados nuevos ficheros en la última comprobación!");
                    }
                }

                // wait a while before checking again for new
                // notifications

                Thread.sleep(500);
            }
        }
        catch (SQLException sqle)
        {
            sqle.printStackTrace();
        }
        catch (InterruptedException ie)
        {
            ie.printStackTrace();
        }
    }
}
