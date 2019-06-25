package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String [] remote_ports =  {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    ArrayList<String> ports = new ArrayList<String>();
    ArrayList<String> dup_hashed_ports = new ArrayList<String>();
    ArrayList<String> hashed_ports = new ArrayList<String>();
    static final int SERVER_PORT = 10000;
    String x ="";
    List<String> sList = new ArrayList<String>();
    List<String> kvList = new ArrayList<String>();
    //List<String> sList = Collections.synchronizedList(new ArrayList<String>());
    boolean flag = true;
    String value_retreived="";
    int count =0;
    String avd_list = "";

    HashMap <String,String> avds = new HashMap<String, String>();

    //Code from OnPTestClickListener.java
    final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String hashed_key = null;
        try {
            hashed_key = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int j =0;
        for (int i=0;i<sList.size();i++){

            if ((hashed_key.compareTo(sList.get(i))<0)){

                String port_insert = avds.get(sList.get(i));

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port_insert, "delete:" + selection);

            }
            else if(j==sList.size()-1){

                String port_insert = avds.get(sList.get(0));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port_insert, "delete:" + selection);

            }
            j++;


        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        //Code from PA1
        String filename = values.getAsString("key");
        String string = values.getAsString("value");



        String gen_port_insert = shuffle(filename, string);

        if (gen_port_insert == null){

            Log.e("Returned Null","Inside null if");

            FileOutputStream outputStream;

            try {
                outputStream = this.getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (FileNotFoundException e) {
                Log.e(TAG, "File write failed");
            } catch (IOException e) {
                e.printStackTrace();
            }

            Log.v("insert", values.toString());

        }

        else {

            Log.e("HashPortToEnterTheKey:", gen_port_insert);

            String port_insert = avds.get(gen_port_insert);
            Log.e("OGPortToEnterTheKey:", port_insert);
            Log.e("Local Port is:", x);
            if (x.equals(port_insert)) {

                //System.out.println("keyfromgrader: "+filename);
                //System.out.println("valuefromgrader: "+string);
                FileOutputStream outputStream;

                try {
                    outputStream = this.getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(string.getBytes());
                    outputStream.close();
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "File write failed");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Log.v("insert", values.toString());
                //return uri;
            } else {

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port_insert, "insert:" + filename + ":" + string);

            }
        }

        return uri;
    }

    private String shuffle(String key, String value) {

        if(flag == true) {
            flag=false;
            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "11108", "chord:" + key + ":" + value);
            try {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "11108", "chord:" + key + ":" + value);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Log.e("Inside shuffle catch","");
                e.printStackTrace();
            }
        }


        Log.e("Shuffled String",avd_list);
        String hashed_key="";
        try {
            hashed_key = genHash(key);
            Log.e("Key received: ",key);
            Log.e("GenHash Key received: ",hashed_key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }



        int j = 0;
        Log.e("Size of sList  Shuffle",sList.size()+"");
        //Log.e("Local all Chord size",sList.size()+"");
        Log.e("sList  Shuffle",sList+"");
        for (int i=0;i<sList.size();i++){

            if ((hashed_key.compareTo(sList.get(i))<0)){

                return sList.get(i);

            }
            else if(j==sList.size()-1){
                return sList.get(0);
            }
            j++;


        }

        return null;
    }

    //return null;


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        ports.add("11108");

        try {
            avds.put(genHash("5554"),"11108");
            avds.put(genHash("5556"),"11112");
            avds.put(genHash("5558"),"11116");
            avds.put(genHash("5560"),"11120");
            avds.put(genHash("5562"),"11124");

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        x = myPort;
        //try {
          //  hashed_port = genHash(x);
        //} catch (NoSuchAlgorithmException e) {
          //  e.printStackTrace();
        //}
        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.e(TAG,"Server socket created");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return true;

        }


        if (!x.equals("11108")){
            Log.e(TAG, "5554 is not the port");

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort,"Join:"+":"+":"+portStr);
            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,ports.get(idxi),"insert:"+key+":"+value);

        }
        else{
            try {
                hashed_ports.add(genHash(portStr));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort + "!" + "Join" + "!" + "0" + "!" + "1" );
        }

        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort,"insert" );


        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub



        //https://stackoverflow.com/questions/9917935/adding-rows-into-cursor-manually
        MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
        String filename = selection;
        String fileList[]=this.getContext().fileList();
        System.out.println("SINGLE AVD IS ALIVE, SIZE OF LIST"+sList.size());
        if (selection.equals("*") || selection.equals("@")) {

            if (sList.size()==0) {
                try {

                    System.out.println("filelist is :" + fileList.length);
                    //ArrayList temp = new ArrayList();
                    for (int i = 0; i < fileList.length; i++) {


                        System.out.println("fetched file" + fileList[i]);
                        InputStream inputStream = getContext().openFileInput(fileList[i]);
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader reader = new BufferedReader(inputStreamReader);
                        String data = reader.readLine();
                        cursor.addRow(new String[]{fileList[i], data});
                        System.out.println("row is:" + fileList[i] + " data is: " + data);


                    }
                    return cursor;
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(selection.equals("*")){

                Log.e("Inside *Multi AVD SIZE:",sList.size()+"");

                for(int i=0;i<sList.size();i++){

                    String original_port = avds.get(sList.get(i));
                    System.out.println("Currently processing port:"+original_port);
                    System.out.println("Current AVD is:"+x);
                    if (original_port.equals(x)){

                        try {

                            System.out.println("filelist is :" + fileList.length);
                            //ArrayList temp = new ArrayList();
                            for (int j = 0; j < fileList.length; j++) {


                                System.out.println("fetched file in *" + fileList[j]);
                                InputStream inputStream = getContext().openFileInput(fileList[j]);
                                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                                BufferedReader reader = new BufferedReader(inputStreamReader);
                                String data = reader.readLine();
                                //if (fileList[j].equals(""))
                                kvList.add(fileList[j]+":"+data);
                                //cursor.addRow(new String[]{fileList[j], data});
                                System.out.println("row is:" + fileList[j] + " data is: " + data);


                            }

                            Log.e("No.of KeyValue in self:",kvList.size()+"");
                            Log.e("key-Value in self",kvList+"");


                            //return cursor;
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }

                    else{

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, original_port, "queryall"+":");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.e("Size key-Value in self",kvList.size()+"");
                Log.e("key-Value in self",kvList+"");

                for(int i=0;i<kvList.size();i++){

                    String [] arr = kvList.get(i).split(":");
                    if (arr.length>1) {
                        cursor.addRow(new String[]{arr[0], arr[1]});
                    }


                }

                return  cursor;


            }

            //@ code

            else{

                //String fileList[]=getContext().fileList();

                try {

                    System.out.println("filelist is :" + fileList.length);
                    //ArrayList temp = new ArrayList();
                    for (int j = 0; j < fileList.length; j++) {


                        System.out.println("fetched file in *" + fileList[j]);
                        InputStream inputStream = getContext().openFileInput(fileList[j]);
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader reader = new BufferedReader(inputStreamReader);
                        String data = reader.readLine();
                        //kvList.add(fileList[j]+":"+data);
                        cursor.addRow(new String[]{fileList[j], data});
                        System.out.println("row is:" + fileList[j] + " data is: " + data);


                    }

                    Log.e("No.of KeyValue in self:",kvList.size()+"");
                    Log.e("key-Value in self",kvList+"");


                    return cursor;
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
        else if (Arrays.asList(fileList).contains(selection)) {

            InputStream inputStream = null;
            try {
                inputStream = getContext().openFileInput(selection);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader reader = new BufferedReader(inputStreamReader);
                String data = reader.readLine();
                cursor.addRow(new String[]{selection, data});
                System.out.println("row inside elseif is:" + selection + " data is: " + data);
                return cursor;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        else{

            int j = 0;
            for (int i=0;i<sList.size();i++){

                System.out.println("Inside for in Query");
                System.out.println("Searching for key: "+selection);
                //int idxi = dup_hashed_ports.indexOf(sList.get(i));
                //int idxj = dup_hashed_ports.indexOf(sList.get(i+1));
            /*
            if ((hashed_key.compareTo(sList.get(i))>0) && (hashed_key.compareTo(sList.get(i+1))<0)){
                //System.out.println("PORT TO PUT THE KEY:"+ports.get(idxj));
                //System.out.println("Key Checked:"+key+":::"+"hashed_key:"+hashed_key);

                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,ports.get(idxi),"insert:"+key+":"+value);
                //ports.get(idxi);
                return sList.get(i+1);


            }
            else if (j == sList.size()-2){

                return sList.get(0);

            }
            j++;
            */

                String hashed_key = null;
                try {
                    hashed_key = genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }

                if ((hashed_key.compareTo(sList.get(i))<0)){

                    String actual_port = avds.get(sList.get(i));

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, actual_port, "query:" + selection);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    System.out.println("Value Found in ring: "+value_retreived);
                    cursor.addRow(new String[] {selection, value_retreived});

                    System.out.println("KEY IN CURSOR:" + selection + " data is: " + value_retreived);

                    return cursor;

                }
                else if(j==sList.size()-1){
                    //return sList.get(0);
                    System.out.println("Value NOT Found in ring: "+value_retreived);
                    String actual_port = avds.get(sList.get(0));
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, actual_port, "query:" + selection);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    cursor.addRow(new String[] {selection, value_retreived});

                    System.out.println("KEY IN CURSOR(zero):" + selection + " data is: " + value_retreived);

                    return cursor;

                }
                j++;


            }



        }


        Log.v("query", selection);
        return null;


    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        //Code from PA1
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

            ContentValues values = new ContentValues();
            String msgToSend = "";
            try {

                while (true) {
                    Socket connection = serverSocket.accept();
                    //https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                    InputStream input = connection.getInputStream();
                    DataInputStream reader = new DataInputStream(input);
                    msgToSend = reader.readUTF();
                    Log.e(TAG, "Message sent from Server: " + msgToSend);

                    String arr[] = msgToSend.split(":");
                    String serveroperation = arr[0];


                    if (serveroperation.equals("delete")){
                        String key = arr[1];

                        getContext().deleteFile(key);





                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);

                        writer.writeUTF("Delete Completed");
                        writer.flush();
                    }


                    if (serveroperation.equals("queryall")){
                        String fileList[]=getContext().fileList();

                        int c =0;

                        String temp ="";

                        for (int j = 0; j < fileList.length; j++) {

                            c++;
                            System.out.println("fetched file in server:" + fileList[j]);
                            InputStream inputStream = getContext().openFileInput(fileList[j]);
                            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                            BufferedReader freader = new BufferedReader(inputStreamReader);
                            String data = freader.readLine();
                            //kvList.add(fileList[j]+":"+data);
                            temp = temp+ fileList[j]+":"+data+",";
                            //cursor.addRow(new String[]{fileList[j], data});

                        }
                        System.out.println("row in server is:" + temp);

                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);

                        writer.writeUTF(temp);
                        writer.flush();

                        Log.e("No. of values inserted:",Integer.toString(c));


                    }


                    if (serveroperation.equals("query")){

                        String key = arr[1];
                        InputStream inputStream = getContext().openFileInput(key);
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader kreader = new BufferedReader(inputStreamReader);
                        String data = kreader.readLine();

                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);

                        writer.writeUTF(data);
                        writer.flush();


                    }

                    if (serveroperation.equals("chord")){

                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);
                        String portlist ="";
                        for (int i=0;i<hashed_ports.size();i++) {
                            portlist = portlist+hashed_ports.get(i)+":";

                        }
                        writer.writeUTF(portlist);
                        writer.flush();
                    }


                    if(serveroperation.equals("Join")){
                        String port = arr[3];

                        //String msg = arr[0];

                        ports.add(port);
                        //dup_ports.add(msg);
                        try {
                            hashed_ports.add(genHash(port));
                            dup_hashed_ports.add(genHash(port));
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }


                        System.out.println("ports are: "+ports);
                        //System.out.println("Message: "+msg);
                        System.out.println("Port" + port);

                    }

                    else if(serveroperation.equals("insert")){

                    String key = arr[1];
                    String value = arr[2];

                    //Code from description
                    /*if ((port.equals("11108")) || (port.equals("11112")) || (port.equals("11116")) ||
                            (port.equals("11120")) || port.equals("11124")){
                        System.out.println("PORT FOUND");
                    }*/

                    values.put("key", key);
                    values.put("value", value);
                    //getContext().getContentResolver().insert(mUri, values);
                    count += 1;

                        //insert_custom(mUri, values);

                        String filename = values.getAsString("key");
                        String string = values.getAsString("value");

                        //shuffle(filename,string);

                        //System.out.println("keyfromgrader: "+filename);
                        //System.out.println("valuefromgrader: "+string);
                        FileOutputStream outputStream;

                        try {
                            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                            outputStream.write(string.getBytes());
                            outputStream.close();
                        } catch (FileNotFoundException e) {
                            Log.e(TAG, "File write failed");
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }

                        Log.v("insert in  if:::", values.toString());



                    }






                    //Sending Acknowledgement of received message to client
                    OutputStream output = connection.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF("Message Received");
                    writer.flush();
                    publishProgress(msgToSend);
                    connection.close();

                }

            }
            catch (IOException e) {
                Log.e(TAG,"Message cannot be sent!");

            }
            return null;
        }


    }



    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {
        //Code from PA1
        @Override
        protected Void doInBackground(String... msgs) {

            /*
             * TODO: Fill in your client code that sends out a message.
             */

            try {

                System.out.println("length of msgs:"+msgs.length);
                String port = msgs[0];
                String temp = msgs[1];
                String arr[] = temp.split(":");
                String operation = arr[0];
                //String key = arr[1];
                //String value = arr[2];
                System.out.println("Message from oncreate:"+temp);

                if(operation.equals("Join")){

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_ports[0]));

                    OutputStream output = socket.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF(operation+":"+port+":"+"Jop"+":"+arr[3]);
                    Log.e(TAG, "Message sent from Client: " + operation);
                    writer.flush();

                    // Based on the discussions in class regarding closing of port after receiving acknowledgment
                    InputStream input = socket.getInputStream();
                    DataInputStream reader = new DataInputStream(input);
                    String ack = reader.readUTF();
                    //Log.e(TAG, "server's response "+ack);
                    if (ack.equals("Message Received")) {
                        Log.e(TAG, "Acknowledgment Received");
                        socket.close();
                    }



                }

                else if(operation.equals("insert")){
                    String key = arr[1];
                    String value = arr[2];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    OutputStream output = socket.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF(operation+":"+key+":"+value);
                    Log.e(TAG, "Message sent from Client: " + key+":"+value);
                    writer.flush();

                    // Based on the discussions in class regarding closing of port after receiving acknowledgment
                    InputStream input = socket.getInputStream();
                    DataInputStream reader = new DataInputStream(input);
                    String ack = reader.readUTF();
                    //Log.e(TAG, "server's response "+ack);
                    if (ack.equals("Message Received")) {
                        Log.e(TAG, "Acknowledgment Received");
                        socket.close();
                    }



                }

                else if(operation.equals("chord")){
                    //String key = arr[1];
                    //String value = arr[2];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));

                    OutputStream output = socket.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF(operation);
                    //Log.e(TAG, "Message sent from Client: " + key+":"+value);
                    writer.flush();

                    // Based on the discussions in class regarding closing of port after receiving acknowledgment
                    InputStream input = socket.getInputStream();
                    DataInputStream reader = new DataInputStream(input);
                    String ack = reader.readUTF();
                    avd_list = ack;
                    //Log.e(TAG, "server's response "+ack);
                   String[] chList = ack.split(":");
                   Arrays.sort(chList);
                   //List<String> sList = new ArrayList<String>();
                    //sList.clear();
                    //synchronized(sList) {
                        //sList.clear();
                        for (String s : chList) {
                            sList.add(s);
                        }

                    //}
                    Log.e("Local all Chord size",sList.size()+"");
                     Log.e("Local all chord",sList+"");




                }


                else  if(operation.equals("query")){

                    String key = arr[1];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    OutputStream output = socket.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF(operation+":"+key);
                    //Log.e(TAG, "Message sent from Client: " + key+":"+value);
                    writer.flush();

                    InputStream input = socket.getInputStream();
                    DataInputStream reader = new DataInputStream(input);
                    value_retreived = reader.readUTF();

                    Log.e("Key Requested: ",key);
                    Log.e("Value Fetched: ",value_retreived);

                }



                else if(operation.equals("queryall")){
                    //String key = arr[1];
                    //String value = arr[2];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    OutputStream output = socket.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF(operation);
                    //Log.e(TAG, "Message sent from Client: " + key+":"+value);
                    writer.flush();

                    // Based on the discussions in class regarding closing of port after receiving acknowledgment
                    InputStream input = socket.getInputStream();
                    DataInputStream reader = new DataInputStream(input);
                    String ack = reader.readUTF();

                    Log.e("Received KV Pair:",ack);
                    //avd_list = ack;
                    //Log.e(TAG, "server's response "+ack);
                    String[] chList = ack.split(",");
                    //Arrays.sort(chList);
                    //List<String> sList = new ArrayList<String>();
                    //sList.clear();
                    //synchronized(sList) {
                    //sList.clear();
                    for (String s : chList) {
                        /*if(s.contains(""))
                            continue;*/
                        kvList.add(s);
                    }


                    //}
                    Log.e("calling server:",port);
                    //Log.e("Local all chord",sList+"");




                }


                else if(operation.equals("delete")){
                    //String key = arr[1];
                    //String value = arr[2];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    OutputStream output = socket.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF(operation+":"+arr[1]);
                    //Log.e(TAG, "Message sent from Client: " + key+":"+value);
                    writer.flush();

                    // Based on the discussions in class regarding closing of port after receiving acknowledgment
                    InputStream input = socket.getInputStream();
                    DataInputStream reader = new DataInputStream(input);
                    String ack = reader.readUTF();
                    Log.e("Delete Output is: ",ack);




                }



            }
            catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }



            return null;
        }

    }


}
