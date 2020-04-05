/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.clases.tarea05ad;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author JoseM
 */
public class Notifier extends Thread{
    private Connection conn;
    private Configuracion configuracion;

    public Notifier(Connection conn, Configuracion configuracion){
        this.conn = conn;
        this.configuracion = configuracion;
    }

    public void run(){
        while (true){
            try{
                File directorio = new File(configuracion.getApp().getDirectory());
                String raiz=directorio.getParent()+directorio.getName();
                MenuTarea menu = new MenuTarea();
                System.out.println("\n~Iniciando comprobación automática de los ficheros.~");
                menu.recorrerFicheros(directorio, conn, raiz);
                System.out.println("Comprobación automática finalizada.");
                Thread.sleep(20000);
            }
            catch (SQLException sqle)
            {
                sqle.printStackTrace();
            }
            catch (InterruptedException ie)
            {
                ie.printStackTrace();
            }
            catch (FileNotFoundException e) {
                System.out.println("No se encuentra el archivo.");
            }
            catch (IOException e) {
                System.out.println("Error de tipo E/S.");
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(Notifier.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
