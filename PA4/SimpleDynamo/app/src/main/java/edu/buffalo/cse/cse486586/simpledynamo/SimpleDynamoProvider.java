package edu.buffalo.cse.cse486586.simpledynamo;

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
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String [] remote_ports =  {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    ArrayList<String> ports = new ArrayList<String>();
    ArrayList<String> dup_hashed_ports = new ArrayList<String>();
    //ArrayList<String> hashed_ports = new ArrayList<String>();
    static final int SERVER_PORT = 10000;
    String x ="";
    String failure ="";
    List<String> sList = new ArrayList<String>();
    List<String> kvList = new ArrayList<String>();
    //List<String> sList = Collections.synchronizedList(new ArrayList<String>());
    boolean failed = false;
    String value_retreived="";
    int count =0;
    String avd_list = "";

    HashMap <String,String> avds = new HashMap<String, String>();
    //HashMap <String,String> kv = new HashMap<String, String>();
    ConcurrentHashMap<String,String> kv = new ConcurrentHashMap<String,String>();

    //Code from OnPTestClickListener.java
    final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

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

            String port_insert = avds.get(sList.get(i));

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port_insert, "delete:" + selection);


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

        /*try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        String filename = values.getAsString("key");
        String string = values.getAsString("value");

        Log.e("KeyVal from grader:",filename+":"+string);

        String gen_port_insert = shuffle(filename, string);

        Log.e("Key:"+filename+":Shuffled Returned:",gen_port_insert);
        String [] repnode = gen_port_insert.split(":");
        Log.e("Length of repnode:",repnode.length+"");



        Log.e("HashPortToEnterTheKey:", gen_port_insert+"");

        for(int i=0;i<repnode.length;i++){

            String port = repnode[i];


            String port_insert = avds.get(port);
            Log.e("OGPortToEnterTheKey:", port_insert);
            Log.e("Local Port is:", x);
            if (x.equals(port_insert)) {

                Log.e(TAG,"Writing in the same Node!");

                //System.out.println("keyfromgrader: "+filename);
                //System.out.println("valuefromgrader: "+string);
                FileOutputStream outputStream;

                try {
                    System.out.println("hi there!!");
                    Log.e("Key:"+filename+":inserted in:"+x+":or:"+port_insert,"abc");
                    outputStream = this.getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(string.getBytes());
                    outputStream.close();
                    System.out.println("hi there2!!");
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "File write failed");
                } catch (IOException e) {
                    Log.e(TAG,"Exception Occured");
                    e.printStackTrace();
                }


                Log.v("inserted key:"+filename, ":in port:"+x);
                //return uri;
            } else {

                Log.e(TAG,"myport:"+x+":Calling new Node!"+port_insert+":for key:"+filename);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port_insert, "insert:" + filename + ":" + string);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }

            Log.e("KV stored","!!!");


        }


        return uri;
    }


    private String shuffle(String key, String value) {


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
        Collections.sort(sList);
        for (int i=0;i<sList.size();i++){
            String portsList = "";

            if ((hashed_key.compareTo(sList.get(i))<0)){

                if(i<3){

                    portsList = sList.get(i)+ ":"+sList.get(i+1)+ ":"+sList.get(i+2);

                    //return sList.get(i);
                    Log.e(TAG,"KeyIs:"+key+":"+"Current port is:"+avds.get(sList.get(i))+":"+"Rep Ports:"+avds.get(sList.get(i+1))+ ":"+avds.get(sList.get(i+2)));
                    return portsList;
                }

                else if(i==3){

                    portsList = sList.get(i)+ ":"+sList.get(i+1)+ ":"+sList.get(0);

                    Log.e(TAG,"KeyIs:"+key+":"+"Current port is:"+avds.get(sList.get(i))+":"+"Rep Ports:"+avds.get(sList.get(i+1))+ ":"+avds.get(sList.get(0)));
                    //return sList.get(i);
                    return portsList;
                }

                else{

                    portsList = sList.get(i)+ ":"+sList.get(0)+ ":"+sList.get(1);

                    //return sList.get(i);
                    Log.e(TAG,"KeyIs:"+key+":"+"Current port is:"+avds.get(sList.get(i))+":"+"Rep Ports:"+avds.get(sList.get(0))+ ":"+avds.get(sList.get(1)));
                    return portsList;

                }


            }
            else if(j==sList.size()-1){

                Log.e("Should never come here","!!!");
                portsList = sList.get(0)+ ":"+sList.get(1)+ ":"+sList.get(2);
                Log.e(TAG,"KeyIs:"+key+":"+"Current port is:"+avds.get(sList.get(0))+":"+"Rep Ports:"+avds.get(sList.get(1))+ ":"+avds.get(sList.get(2)));
                return portsList;

            }
            j++;


        }
        Log.e(TAG,"returning null ports");
        return null;
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        try {
            avds.put(genHash("5554"),"11108");
            avds.put(genHash("5556"),"11112");
            avds.put(genHash("5558"),"11116");
            avds.put(genHash("5560"),"11120");
            avds.put(genHash("5562"),"11124");

            sList.add(genHash("5554"));
            sList.add(genHash("5556"));
            sList.add(genHash("5558"));
            sList.add(genHash("5560"));
            sList.add(genHash("5562"));

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Collections.sort(sList);

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        x = myPort;
        Log.e("myport:",myPort);



        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.e(TAG,"Server socket created");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return true;

        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort,"clean"+":"+portStr);


        return false;

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        //https://stackoverflow.com/questions/9917935/adding-rows-into-cursor-manually
        MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
        String filename = selection;
        String fileList[]=this.getContext().fileList();
        //System.out.println("SINGLE AVD IS ALIVE, SIZE OF LIST"+sList.size());
        if (selection.equals("*") || selection.equals("@")) {


            if(selection.equals("*")){

                Log.e("Checking * Query:",sList.size()+"");

                for(int i=0;i<sList.size();i++){

                    String original_port = avds.get(sList.get(i));
                    
                    Log.e("Current Port:"+x,":Porcessing Port:"+original_port);
                    if (original_port.equals(x)){

                        try {
                            Log.e(TAG,"Getting values from self");
                            
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
                        Log.e("Getting values from:",original_port);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, original_port, "queryall"+":");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                }
                Log.e(TAG,"Gathered All KeyValue Pairs");
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
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.e(TAG,"Checking @ Query");

                //String fileList[]=getContext().fileList();

                try {

                    System.out.println("filelist is :" + fileList.length);
                    //ArrayList temp = new ArrayList();
                    for (int j = 0; j < fileList.length; j++) {


                        System.out.println("fetched file in @" + fileList[j]);
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

            int j = -1;
            for (int i=0;i<sList.size();i++){

                j++;
                System.out.println("Inside for in Query");
                System.out.println("Searching for key: "+selection);


                String hashed_key = null;
                try {
                    hashed_key = genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }

                if ((hashed_key.compareTo(sList.get(i))<0)){

                    String actual_port = avds.get(sList.get(i));

                    Log.e("Key:"+selection+":","Found in port:"+actual_port);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, actual_port, "query:" + selection);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Log.e("Recovery result:",failure);
                    if(failure.equals("")) {
                        System.out.println("Value Found in ring: " + kv.get(selection));
                        cursor.addRow(new String[]{selection, kv.get(selection)});

                        System.out.println("KEY IN CURSOR:" + selection + " data is: " + kv.get(selection));

                        return cursor;
                    }
                    if(i==sList.size()-1 && failure.equals("f")){

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, avds.get(sList.get(0)), "query:" + selection);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        System.out.println("Value Found in last: " + kv.get(selection));
                        cursor.addRow(new String[]{selection, kv.get(selection)});

                        System.out.println("KEY IN CURSOR:" + selection + " data is: " + kv.get(selection));

                        return cursor;

                    }


                }
                else if(j==sList.size()-1) {
                    //return sList.get(0);
                    for (int z = 0; z < 3; z++){
                        System.out.println("Value NOT Found in ring: " + value_retreived);
                        String actual_port = avds.get(sList.get(z));
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, actual_port, "query:" + selection);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        Log.e("Recovery result:", failure);
                        if (failure.equals("")) {
                            cursor.addRow(new String[]{selection, kv.get(selection)});

                            System.out.println("KEY IN CURSOR(zero):" + selection + " data is: " + kv.get(selection));

                            return cursor;
                        }
                        failure="";
                    }

                }
                
                failure="";

            }
        }


        Log.v("query", selection);
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
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


                    if(serveroperation.equals("recover")){

                        Log.e(TAG,"Inside Recovery");

                        String key_val = arr[2];
                        Log.e("KP->",key_val);
                        String[] chList = key_val.split(",");
                        Log.e("List to iterate:::",Arrays.toString(chList) );

                        for(int i = 0; i<chList.length; i++) {

                            Log.e("Processing:::",chList[i]);

                            String[] temp = chList[i].split("@");


                            if (temp.length > 1) {
                                Log.e(TAG,"Inserting:"+temp[0]+":"+temp[1]);
                                String key = temp[0];
                                String value = temp[1];


                                values.put("key", key);
                                values.put("value", value);

                                String filename = values.getAsString("key");
                                String string = values.getAsString("value");

                                FileOutputStream outputStream;

                                try {
                                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                    outputStream.write(string.getBytes());
                                    outputStream.close();
                                } catch (FileNotFoundException e) {
                                    Log.e(TAG, "File write failed");
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                //Log.v("inserted key:"+filename, ":in port:"+arr[3]);
                                Log.e("Key:" + filename + ":inserted in:", arr[1]);
                                //Log.v("insert in  if:::", values.toString());

                            }
                        }

                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);


                        Log.e(TAG,"Files written!");
                        writer.writeUTF("Recovery Complete");
                        writer.flush();




                    }


                    if(serveroperation.equals("GetKV")){

                        String port = arr[1];
                        String FL = "";
                        String fileList[]=getContext().fileList();

                        for(int i = 0; i < fileList.length; i++){

                            Log.e(TAG,"Calling Shuffle with:"+fileList[i]);

                            String nodes = shuffle(fileList[i],"");

                            Log.e("Nodes retreived:",nodes);
                            String temp [] = nodes.split(":");

                            for (int j = 0; j<temp.length;j++){
                                Log.e("Port Found:",avds.get(temp[j])+":portcompare:"+port);
                                if(port.equals(avds.get(temp[j]))){

                                    FileOutputStream outputStream;

                                    InputStream inputStream = getContext().openFileInput(fileList[i]);
                                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                                    BufferedReader kreader = new BufferedReader(inputStreamReader);
                                    String data = kreader.readLine();
                                    Log.e("KeyVal Pair in recover:",fileList[i]+":"+data);

                                    FL = FL + fileList[i] + "@" + data + ",";

                                    Log.e("Key-Value recovered:",FL);

                                }
                            }



                        }
                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);


                        Log.e("File List sent!",FL);
                        writer.writeUTF(FL);
                        writer.flush();

                    }


                    if (serveroperation.equals("GetCount")){

                        String fileList[]=getContext().fileList();
                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);

                        writer.writeUTF(Integer.toString(fileList.length));
                        writer.flush();

                    }


                    if (serveroperation.equals("delete")){
                        String fileList[]=getContext().fileList();
                        String key = arr[1];

                        for (int j = 0; j < fileList.length; j++) {

                            if(key.equals(fileList[j])){

                                getContext().deleteFile(key);

                            }

                        }

                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);

                        writer.writeUTF("Delete Completed");
                        writer.flush();
                    }


                    if (serveroperation.equals("clean")){
                        String fileList[]=getContext().fileList();
                        //String key = arr[1];
                        Log.e("Files Left previously:",fileList.length+"");
                        if (fileList.length>0) {
                            for (int j = 0; j < fileList.length; j++) {

                                getContext().deleteFile(fileList[j]);

                            }
                        }

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
                        Log.e(TAG, "Inside Server Query!!");
                        //String data ="a";
                        String key = arr[1];



                        InputStream inputStream = getContext().openFileInput(key);
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader kreader = new BufferedReader(inputStreamReader);
                        String data = kreader.readLine();
                        Log.e("Value for the key-" + key + ":", ":found is:" + data);
                        //break;


                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);


                        Log.e("found val for key:" + key + ":val:", data);
                        writer.writeUTF(data);
                        writer.flush();


                    }


                    else if(serveroperation.equals("insert")){

                        Log.e(TAG,"INSIDE SERVER");

                        String key = arr[1];
                        String value = arr[2];

                        Log.e(TAG, "KeyValue received in Server: " + key+":"+value);



                        values.put("key", key);
                        values.put("value", value);
                        //getContext().getContentResolver().insert(mUri, values);
                        count += 1;

                        //insert_custom(mUri, values);

                        String filename = values.getAsString("key");
                        String string = values.getAsString("value");

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
                        //Log.v("inserted key:"+filename, ":in port:"+arr[3]);
                        Log.e("Key:"+filename+":inserted in:",arr[3]);
                        //Log.v("insert in  if:::", values.toString());

                        //Sending Acknowledgement of received message to client
                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);
                        writer.writeUTF("Message Received");
                        writer.flush();
                        publishProgress(msgToSend);
                        connection.close();



                    }

                    else {

                        //Sending Acknowledgement of received message to client
                        OutputStream output = connection.getOutputStream();
                        DataOutputStream writer = new DataOutputStream(output);
                        writer.writeUTF("Message Received");
                        writer.flush();
                        publishProgress(msgToSend);
                        connection.close();
                    }

                }

            }
            catch (IOException e) {
                Log.e(TAG,"Message cannot be sent!");

            }
            return null;
        }


    }


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
                System.out.println("Port in client:"+port);
                String temp = msgs[1];
                String arr[] = temp.split(":");
                Log.e("Array after splitting:",arr+"");
                String operation = arr[0];
                
                System.out.println("Message in Client:"+operation);




                if(operation.equals("clean")){
                    Log.e(TAG,"Inside Clean in Client");

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    String count;
                    //ArrayList<String> kvalues = new ArrayList<String>();
                    ArrayList<String> counts = new ArrayList<String>();
                    String kvalues = "";
                    Boolean recovery = false;

                    OutputStream output = socket.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF(operation);
                    Log.e(TAG, "Message sent from Client: " + operation);
                    writer.flush();

                    // Based on the discussions in class regarding closing of port after receiving acknowledgment
                    InputStream input = socket.getInputStream();
                    DataInputStream reader = new DataInputStream(input);
                    String ack = reader.readUTF();
                    //Log.e(TAG, "server's response "+ack);
                    if (ack.equals("Delete Completed")) {
                        Log.e(TAG, "Clean, Acknowledgment Received");
                        //socket.close();
                    }

                    Log.e(TAG,"Checking if recovery!");

                    for (int i = 0; i<remote_ports.length;i++) {


                        if(!remote_ports[i].equals(port)) {
                            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remote_ports[i]));

                            OutputStream output1 = socket1.getOutputStream();
                            DataOutputStream writer1 = new DataOutputStream(output1);
                            writer1.writeUTF("GetCount");
                            Log.e(TAG, "Message sent from Client: " + "GetCount");
                            writer1.flush();

                            InputStream input1 = socket1.getInputStream();
                            DataInputStream reader1 = new DataInputStream(input1);
                            count = reader1.readUTF();
                            Log.e("Count from port:"+remote_ports[i],":"+count);
                            counts.add(count);
                            if(!count.equals("") || !count.equals(null)){
                                socket1.close();
                            }
                        }

                    }
                    Log.e("Count from each avd:",counts+"");
                    for(String i : counts){

                        if(Integer.parseInt(i)>0){
                            recovery = true;
                            Log.e(TAG,"Confirmed, its recovery");
                            break;
                        }

                    }

                    if(recovery){

                        Log.e(TAG,"Inside Client Recovery!");

                        for (int i = 0; i<remote_ports.length;i++) {
                            String kv = "";
                            if(!remote_ports[i].equals(port)) {

                                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remote_ports[i]));

                                OutputStream output2 = socket2.getOutputStream();
                                DataOutputStream writer2 = new DataOutputStream(output2);
                                writer2.writeUTF("GetKV"+":"+port);
                                Log.e(TAG, "Message sent from Client: " + "GetKV");
                                writer2.flush();

                                InputStream input2 = socket2.getInputStream();
                                DataInputStream reader2 = new DataInputStream(input2);
                                kv = reader2.readUTF();
                                Log.e("KeyValue from "+remote_ports[i],":"+kv);
                                //kv = kv+",";
                                kvalues = kvalues+kv;

                            }
                        }

                        Log.e("Key-Value pairs:",kvalues);

                        
                        ContentValues values = new ContentValues();
                        String[] chList = kvalues.split(",");
                        Log.e("List to iterate:::",Arrays.toString(chList) );

                        for(int i = 0; i<chList.length; i++) {

                            Log.e("Processing:::",chList[i]);

                            String[] temp1 = chList[i].split("@");


                            if (temp1.length > 1) {
                                Log.e(TAG,"Inserting:"+temp1[0]+":"+temp1[1]);
                                String key = temp1[0];
                                String value = temp1[1];


                                values.put("key", key);
                                values.put("value", value);

                                String filename = values.getAsString("key");
                                String string = values.getAsString("value");

                                FileOutputStream outputStream;

                                try {
                                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                    outputStream.write(string.getBytes());
                                    outputStream.close();
                                } catch (FileNotFoundException e) {
                                    Log.e(TAG, "File write failed");
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                //Log.v("inserted key:"+filename, ":in port:"+arr[3]);
                                Log.e("Key:" + filename + ":inserted in:", port);
                                //Log.v("insert in  if:::", values.toString());

                            }
                        }

                    }

                }



                if(operation.equals("insert")){
                    Log.e(TAG,"Inside Client Insert");
                    String key = arr[1];
                    String value = arr[2];
                    Log.e("KeyValue from Insert:",key+":"+value);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    OutputStream output = socket.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF(operation+":"+key+":"+value+":"+port);
                    //Log.v("inserted key:"+key, ":in port:"+arr[3]);
                    Log.e(TAG, "KeyValue sent from Client: " + key+":"+value);
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

                else  if(operation.equals("query")){

                    Log.e(TAG, "Inside Client Query!!");

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
                    kv.put(key,value_retreived);


                    Log.e("Key Requested: ",key);
                    Log.e("Value Fetched: ",value_retreived);

                    //kv.put(key,value_retreived);

                }



                else if(operation.equals("queryall")){
                    
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

                    String[] chList = ack.split(",");

                    for (String s : chList) {
                        
                        kvList.add(s);
                    }


                    //}
                    Log.e("calling server:",port);

                }


                else if(operation.equals("delete")){
                    
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
                failed = true;
                failure = "f";
                Log.e("Set Failure as:",failure);
                Log.e(TAG, "ClientTask socket IOException");
                //e.printStackTrace();
            }


            return null;
        }

    }


}
