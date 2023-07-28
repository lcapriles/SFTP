package com.util.SFTPget;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

public class GetSFTP {
  public static void main(String[] args) throws IOException {
    String hostFTP = args[0];
    int portFTP = Integer.parseInt(args[1]);
    String userFTP = args[2];
    String pwdFTP = args[3];
    String remotePath = args[4];
    String newRemotePath = args[5];
    String localPath = args[6];
    String ficheroLog = args[7];

    JSch jsch = new JSch();
    Channel channelSftp = null;
    ChannelSftp cSftp = null;	

    Writer out = new BufferedWriter(new FileWriter(ficheroLog, true));
    try {
	   
      DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
      Date date = new Date();
      out.append("Inicio*** Fecha: " + dateFormat.format(date) + "\n");
      out.append("Estableciendo conexion servidor: " + hostFTP + ":" + portFTP + " - usuario: " + userFTP + "... ");
      Session session = jsch.getSession(userFTP, hostFTP, portFTP);
      session.setPassword(pwdFTP);
      session.setConfig("StrictHostKeyChecking", "no");
      session.connect(10000);
      out.append("OK \n");
      out.append("Creando canal de comunicacion... ");
      channelSftp = session.openChannel("sftp");
      channelSftp.connect();
      cSftp = (ChannelSftp)channelSftp;
      out.append("OK \n");
      out.append("Moviendonos al directorio remoto: " + remotePath + "... ");
      cSftp.cd(remotePath);
      out.append("OK \n");
      out.append("Obteniendo ficheros a descargar de la ruta remota " + remotePath + "... ");
      Vector<ChannelSftp.LsEntry> filesToDownload = cSftp.ls(remotePath);
      out.append("OK \n");
	  
      Boolean descargamos = null;
      for (int i = 0; i < filesToDownload.size(); i++) {
        descargamos = Boolean.valueOf(false);
        ChannelSftp.LsEntry fileToDownload = filesToDownload.get(i);
        if (!fileToDownload.getAttrs().isDir()) {
          descargamos = Boolean.valueOf(true);
          out.append("Descargando fichero: " + fileToDownload.getFilename() + " en " + localPath + "... ");
          OutputStream output = new FileOutputStream(localPath + fileToDownload.getFilename());
          cSftp.get(fileToDownload.getFilename(), output);
          output.close();
          File localFile = new File(localPath + fileToDownload.getFilename());
          long localSize = localFile.length();
          long remoteSize = fileToDownload.getAttrs().getSize();
          if (localSize == remoteSize) {
            out.append("OK \n");
            //out.append("Moviendo fichero a directorio " + newRemotePath + "... ");
            out.append("Borrando remoto " + fileToDownload.getFilename() + "... ");
            //DateFormat dateFormatMove = new SimpleDateFormat("yyyyMMdd_HHmmss_");
            //Date dateMove = new Date();
            //cSftp.rename(remotePath + fileToDownload.getFilename(), newRemotePath + dateFormatMove.format(dateMove) + fileToDownload.getFilename());
            cSftp.rm(fileToDownload.getFilename());
            out.append("OK \n");
          } else {
            out.append("ERROR. No coinciden tama\n");
          } 
        } 
      } 
      if (descargamos.booleanValue()) {
        out.append("Todos los archivos se han descargado correctamente!\n");
      } else {
        out.append("No hay ningarchivo para descargar en el directorio " + remotePath + "\n");
      } 
      out.append("Cerrando canal y desconectandonos del servidor... ");
      cSftp.exit();
      session.disconnect();
      out.append("OK\n");

      date = new Date();
      out.append("Fin*** Fecha: " + dateFormat.format(date) + "\n\n");  
      out.close();
    } catch (Exception e) {
      out.append("\n" + e.toString() + "\n\n");
      out.close();
    } 
    System.exit(0);
  }
}
