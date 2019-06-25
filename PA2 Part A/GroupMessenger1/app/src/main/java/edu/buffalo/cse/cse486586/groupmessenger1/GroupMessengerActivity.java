package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.net.Uri;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.telephony.TelephonyManager;
import android.content.Context;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import android.os.AsyncTask;
import android.util.Log;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;

import static android.content.ContentValues.TAG;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */

public class GroupMessengerActivity extends Activity {
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String [] remote_ports =  {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    private Button send;
    private EditText msg;
    int count = 0;

    //Code from OnPTestClickListener.java
    final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        //Code from PA1
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.e(TAG,"Server socket created");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;

        }

        final TextView tv = (TextView) findViewById(R.id.textView1);

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        send = (Button) findViewById(R.id.button4);
        send.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                msg = (EditText) findViewById(R.id.editText1);
                String text = msg.getText().toString();
                msg.setText("");
                //TextView localTextView = (TextView) findViewById(R.id.textView1);
                tv.append("\t" + text); // This is one way to display a string.
                tv.append("\n");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, text, myPort);


            }
        });

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
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
                    //Code from description
                    values.put("key", Integer.toString(count));
                    values.put("value", msgToSend);
                    getContentResolver().insert(mUri, values);
                    count += 1;

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

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            //Code from PA1
            String strReceived = strings[0].trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strReceived + "\t\n");

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */

            String filename = "SimpleMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }

            return;
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

            for (int i = 0; i < remote_ports.length; i++) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_ports[i]));

                    String msgToSend = msgs[0];
                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */

                    OutputStream output = socket.getOutputStream();
                    DataOutputStream writer = new DataOutputStream(output);
                    writer.writeUTF(msgToSend);
                    Log.e(TAG, "Message sent from Client: " + msgToSend);
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
                catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }


                }



            return null;
        }

    }

}