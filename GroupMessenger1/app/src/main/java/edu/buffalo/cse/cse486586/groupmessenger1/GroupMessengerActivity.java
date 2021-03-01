package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final int[] REMOTE_PORT = {11108, 11112, 11116, 11120, 11124};
    static final int SERVER_PORT = 10000;
    static int keyInc = 0;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        tv.append("\n");
        
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

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.v("Port",myPort);

        try {
            Log.v("call","Calling Server Socket");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.v("socket","Server socket created");
        } catch (IOException e) {
            Log.v("Error", "Can't create screenshot");
            return;
        }

        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button sendButton = (Button) findViewById(R.id.button4);

        sendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.v("Click","Send Clicked");
                String msg = editText.getText().toString();
                editText.setText("");

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        });
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];

            try {

                while (true){
                    Log.v("Server","This is server");
                    Socket socket = serverSocket.accept();
//                    PrintWriter outClient = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
//                    BufferedReader inClient = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
//                    String msg = inClient.readLine();

                    InputStream inputStream = socket.getInputStream();
                    Log.v("Server","Error1");
                    ObjectInputStream ois = new ObjectInputStream(inputStream);
                    Log.v("Server","Error2");
//                    String msg = (String) ois.readObject();
                    Message message = (Message) ois.readObject();
                    Log.v("Server","Error3");


//                    System.out.println("Message: " + msg);
//                    System.out.println("Message: " + message.msg);
                    System.out.println("Sequence no.: " + message.seq);

                    if(message.msg == null) break;
                    publishProgress(message.msg);
//                    outClient.println(msg + "Received");
//                    outClient.flush();
//                    outClient.close();
//                    inClient.close();
                    ois.close();
                    socket.close();
                }

            } catch (IOException e) {
                Log.e("Server error" , "Failed to receive message");
                e.printStackTrace();
            } catch (ClassNotFoundException c){
                Log.e("Server error" , "Class not found");
            }

            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {

            String strReceived = values[0].trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strReceived + "\t\n");

            ContentValues valuesToInsert = new ContentValues();
            valuesToInsert.put("key", Integer.toString(keyInc));
            valuesToInsert.put("value", strReceived);

            Uri uri = getContentResolver().insert(
              Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger1.provider"),
              valuesToInsert
            );

            Cursor res = getContentResolver().query(Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger1.provider"),null,"key",null,null);
            System.out.println("Result = " + res.getCount());
            keyInc++;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try{
                int i = 0;

                while(i<5){
                    Log.v("Created","Client created");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            REMOTE_PORT[i]);

//                    String msgToSend = msgs[0];
                    OutputStream outputStream = socket.getOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
                    System.out.println(msgs[0]);
                    Message message = new Message(msgs[0]);
//                    message.msg = msgs[0];

//                    OutputStream outStream = socket.getOutputStream();
//                    InputStream inStream = socket.getInputStream();
//
//                    PrintWriter outServer = new PrintWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));
//                    BufferedReader inServer = new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8));
//
//                    outServer.println(msgToSend);
//                    outServer.flush();
//                    String reply = inServer.readLine();
//
//                    outServer.close();
//                    inServer.close();


                    oos.writeObject(message);

                    oos.close();
                    socket.close();

                    i++;
//                    Log.v("Completed",reply);
                }
            } catch (UnknownHostException e) {
                Log.e("Exception", "ClientTask UnknownHostException");
            } catch (IOException e){
                Log.e("Exception", "ClientTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}

class Message implements Serializable {
    private static final long serialVersionUID = -299482035708790407L;

    int seq;
    public String msg;

    Message(String msg){
        this.msg = msg;
    }
}
