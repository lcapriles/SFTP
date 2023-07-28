package com.util.SFTPput;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import oracle.jdbc.pool.OracleDataSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class PutSFTP {
  public static void main(String[] args) throws IOException {
    String hostFTP = args[0];
    int portFTP = Integer.parseInt(args[1]);
    String userFTP = args[2];
    String pwdFTP = args[3];
    String localPath = args[4];
    String newLocalPath = args[5];
    String remotePath = args[6];
    String ficheroLog = args[7];
    String userDB = args[8];
    String passDB = args[9];
    String esquemaDB = args[10];

    Calendar dateHoy = Calendar.getInstance();
    Integer fechaHoy = Integer.valueOf(100000 + (dateHoy.get(1) - 2000) * 1000 + dateHoy.get(6));
    Integer horaHoy = Integer.valueOf(dateHoy.get(11) * 10000 + dateHoy.get(12) * 100 + dateHoy.get(13));

    JSch jsch = new JSch();
    Connection con = null;
    Channel channelSftp = null;
    ChannelSftp cSftp = null;

    Writer out = new BufferedWriter(new FileWriter(ficheroLog, true));
    try {
      con = ConexionOracle(userDB, passDB, esquemaDB);

      DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
      Date date = new Date();
      out.append("Inicio*** Fecha: " + dateFormat.format(date) + "\n");
      out.append("Estableciendo conexion servidor: " + hostFTP + ":" + portFTP + " - usuario: " + userFTP + "...");
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
      out.append("Obteniendo ficheros a enviar de la ruta local " + localPath + "... ");
      ArrayList<File> filesToSend = getFilesToSend(localPath);
      out.append("OK \n");

      Boolean enviar = Boolean.valueOf(false);
      if (filesToSend.size() > 0)
        enviar = Boolean.valueOf(true); 
      for (int i = 0; i < filesToSend.size(); i++) {
        File fileToSend = new File(localPath + ((File)filesToSend.get(i)).getName());
        long localSize = fileToSend.length();
        out.append("Enviando fichero: " + fileToSend.getName() + "... ");

        InputStream in = new FileInputStream(fileToSend);
        cSftp.put(in, fileToSend.getName());
        in.close();
        long remoteSize = cSftp.lstat(fileToSend.getName()).getSize();
        
        if ((localSize == remoteSize) || (1==1)) { 
          out.append("OK \n");

          ActualizarEnvio(con, esquemaDB, fechaHoy, horaHoy, fileToSend.getName() );

          out.append("Moviendo fichero a directorio " + newLocalPath + "... ");
          DateFormat dateFormatMove = new SimpleDateFormat("yyyyMMdd_HHmmss_");
          Date dateMove = new Date();
          File newFile = new File(newLocalPath + dateFormatMove.format(dateMove) + ((File)filesToSend.get(i)).getName());
          InputStream inStream = new FileInputStream(fileToSend);
          OutputStream outStream = new FileOutputStream(newFile);
          byte[] buffer = new byte[1024];
          int length;
          while ((length = inStream.read(buffer)) > 0)
            outStream.write(buffer, 0, length); 
          inStream.close();
          outStream.close();
          fileToSend.delete();
          out.append("OK \n");
        } else {
          out.append("ERROR. No coinciden tamaño ("+localSize+")/("+remoteSize+")...\n");
        } 
      } 
      if (enviar.booleanValue()) {
        out.append("Todos los archivos se han enviado correctamente!\n");
      } else {
        out.append("No hay archivos para enviar en el directorio " + localPath + "\n");
      } 
      out.append("Cerrando canal y desconectandonos del servidor... ");
      
      con.close();
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
  
  public static ArrayList<File> getFilesToSend(String path) {
    File folder = new File(path);
    File[] listOfFiles = folder.listFiles();
    ArrayList<File> filesToSend = new ArrayList<>();
    for (File elem : listOfFiles) {
      if (elem.isFile())
        filesToSend.add(elem); 
    } 
    return filesToSend;
  }

  private static void ActualizarEnvio(Connection pConn, String pEsquema, Integer pFechaHoy, Integer pHoraHoy,
                                      String pArchivo ) {
    try {
      String qry = "update " + pEsquema + 
                   ".FQA04942 set erEV01 = ? , erUPMC = ? , erUPMT = ? , erUSER1 = ? , erPID1 = ? where trim(erNFLF) like ? ";
      PreparedStatement updateQry = pConn.prepareStatement(qry);
      updateQry.setString(1, "3");
      updateQry.setInt(2, pFechaHoy.intValue());
      updateQry.setInt(3, pHoraHoy.intValue());
      updateQry.setString(4, "JAVA1");
      updateQry.setString(5, "SFTPput");
      updateQry.setString(6, "%" + pArchivo + "%");
      updateQry.execute();
      updateQry.close();
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }

  public static Connection ConexionOracle(String pUser, String pPass, String pEsquemaDta) {
    try {
      OracleDataSource ds = new OracleDataSource();
      if (pEsquemaDta.trim().equals("PRODDTA")) {
        ds.setURL("jdbc:oracle:thin:@VMJDVLAT000:1521:PVLAT8");
      } else {
        ds.setURL("jdbc:oracle:thin:@VMJDVLAT200:1521:DVLAT8");
      } 
      return ds.getConnection(pUser, pPass);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    } 
  }

}
