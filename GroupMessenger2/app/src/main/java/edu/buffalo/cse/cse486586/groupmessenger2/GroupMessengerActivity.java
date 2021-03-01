package edu.buffalo.cse.cse486586.groupmessenger2;

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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final int[] REMOTE_PORT = {11108, 11112, 11116, 11120, 11124};
    static final int SERVER_PORT = 10000;
    static int seqNo = 0;
    static int myPort;
    static int keyInc = 0;
    Comparator<Message> comparator = new SeqComparator();
    PriorityQueue<Message> queue = new PriorityQueue<Message>(10, comparator);

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
        myPort = (Integer.parseInt(portStr) * 2);

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.v("call","Calling Server Socket");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.v("socket","Server socket created");
        } catch (IOException e) {
            Log.v("Error", "Can't create screenshot");
            e.printStackTrace();
            return;
        }

        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button sendButton = (Button) findViewById(R.id.button4);

        sendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
//                Log.v("Click","Send Clicked");
                String msg = editText.getText().toString();
                editText.setText("");

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        });
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            int clientPort = 0;
            int proposedSeq;
            Iterator<Message> iterator;
            Message next_msg;
            Message publishMessage;

            while(true) {
                try {


//                    System.out.println("Queue size: " + queue.size());


//                    Log.v("Server", "This is server");
                    Socket socket = serverSocket.accept();
//                    Log.v("Server", "1");
                    OutputStream outputStream = socket.getOutputStream();
//                    Log.v("Server", "2");
                    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
//                    Log.v("Server", "3");
                    InputStream inputStream = socket.getInputStream();
//                    Log.v("Server", "4");
                    ObjectInputStream ois = new ObjectInputStream(inputStream);
//                    Log.v("Server", "5");
                    Message message = (Message) ois.readObject();
//                    Log.v("Server", "6");

                    clientPort = message.myPort;

                    message.deliverable = false;
                    queue.add(message);
                    oos.writeObject(message);
                    oos.flush();

                    Message new_message = (Message) ois.readObject();
                    iterator = queue.iterator();
                    while(iterator.hasNext()){
                        next_msg = iterator.next();
                        if(new_message.seq == next_msg.seq && new_message.myPort == next_msg.myPort){
                            queue.remove(next_msg);
                            new_message.deliverable = true;
                            queue.add(new_message);
                            break;
                        }
                    }
                    publishMessage = new_message;

                    while (queue.peek() != null && queue.peek().deliverable){
                        publishMessage = queue.poll();


                        ContentValues valuesToInsert = new ContentValues();
                        valuesToInsert.put("key", Integer.toString(keyInc));
                        valuesToInsert.put("value", publishMessage.msg.trim());
                        publishMessage.key = keyInc;

                        Uri uri = getContentResolver().insert(
                                Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider"),
                                valuesToInsert);

//                        Cursor res = getContentResolver().query(Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider"),null,"key",null,null);


                        keyInc++;

                    }
                    publishProgress(publishMessage);

//                    if (message.seq == 0) {
//                        Log.v("Delivered", "False");
//                        message.deliverable = false;
//                        queue.add(message);
//                        System.out.println("Queue after adding: " + queue);
//                        List<Integer> lastMax = new ArrayList<Integer>();
//                        lastMax.add(lastProposedSeq);
//                        lastMax.add(lastSeq);
//                        proposedSeq = Collections.max(lastMax);
//                        lastProposedSeq = proposedSeq + 1;
//                        Message proposal = new Message(message.msg, proposedSeq + 1);
//                        oos.writeObject(proposal);
//
//                        oos.close();
//                        ois.close();
//                        socket.close();
//                    } else {
//                        Log.v("Delivered", "True");
//                        message.deliverable = true;
//                        queue.add(message);
//                        System.out.println("Queue after adding: " + queue);
//
////                        if(message.msg == null) break;
////                        publishProgress(message.msg);
//                    }
//
//                    oos.close();
//                    ois.close();
//                    socket.close();
//
//                    Message msgToSend = queue.remove();
//                    if (msgToSend.deliverable) {
//                        publishProgress(msgToSend);
//                    }


                } catch (SocketTimeoutException e) {
                    Log.e("Exception", "ServerTask SocketTimeoutException");
                } catch (StreamCorruptedException e) {
                    Log.e("Exception", "ServerTask StreamCorruptedException");
                } catch (EOFException e) {
                    Log.e("Exception", "ServerTask EOFException");
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.e("Exception", "ServerTask IOException");
                    e.printStackTrace();
                } catch (Exception e) {
                    Log.e("Exception", "ServerTask General Exception");
                } finally {
                    Iterator<Message> iter = queue.iterator();
                    Message deleteMsg;
                    while (iter.hasNext()) {
                        deleteMsg = iter.next();
                        if (deleteMsg.myPort == clientPort) {
                            queue.remove(deleteMsg);
                        }
                    }
                }
            }
        }

        @Override
        protected void onProgressUpdate(Message... message) {
            String strReceived = message[0].msg.trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strReceived + " : " + message[0].key + " , " + message[0].myPort + "\t\n");


        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {

                Socket sockets[] = new Socket[5];
                Socket socket;
                ObjectInputStream inputStrems[] = new ObjectInputStream[5];
                ObjectOutputStream outputStrems[] = new ObjectOutputStream[5];
                seqNo++;
                List<Message> messageList = new ArrayList<Message>();
                Message message = new Message("",0,0);

                for(int i=0;i<5;i++){
                    try{
//                        Log.v("Created","Client created");
                        sockets[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                REMOTE_PORT[i]);
                        socket = sockets[i];
//                        Log.v("Connected","Client Connected");

//                    String msgToSend = strings[0];
                        outputStrems[i] = new ObjectOutputStream(socket.getOutputStream());
                        inputStrems[i] = new ObjectInputStream(socket.getInputStream());
                        ObjectOutputStream oos = outputStrems[i];
                        ObjectInputStream ois = inputStrems[i];

                        socket.setSoTimeout(2000);

                        message = new Message(strings[0], seqNo, myPort);

                        oos.writeObject(message);
                        oos.flush();

                        Message proposedMsg = (Message) ois.readObject();
                        messageList.add(proposedMsg);
                    }
                    catch (SocketTimeoutException e){
                        Log.e("SocketTimeoutException", message.msg);
                    } catch (StreamCorruptedException e){
                        Log.e("StreamCorruptedEx", message.msg);
                    } catch (EOFException e){
                        Log.e("EOFException", message.msg);
                    } catch (UnknownHostException e) {
                        Log.e("UnknownHostException", message.msg);
                    }catch (IOException e){
                        Log.e("IOException", message.msg);
                        e.printStackTrace();
                    } catch (ClassNotFoundException e){
                        Log.e("ClassNotFoundException", message.msg);
                    } catch (Exception e){
                        Log.e("Exception", message.msg);
                    }
                }

                Message msgToSend = messageList.get(0);
                int maxSeqNo = seqNo;
                for(Message m: messageList){
                    if(m.proposedSeqNo > maxSeqNo){
                        maxSeqNo = m.proposedSeqNo;
                        msgToSend = m;
                    }
                }
                seqNo = maxSeqNo;
                msgToSend.finalSeqNo = seqNo;

                for(int i=0;i<5;i++){
                    try{
                        Log.v("Created","Proposal Client created");
                        socket = sockets[i];
                        ObjectOutputStream oos = outputStrems[i];
                        oos.writeObject(msgToSend);
                        oos.flush();

                        socket.close();
                    }

                    catch (SocketTimeoutException e){
                        Log.e("SocketTimeoutException", msgToSend.msg);
                    } catch (StreamCorruptedException e){
                        Log.e("StreamCorruptedEx", msgToSend.msg);
                    } catch (EOFException e){
                        Log.e("EOFException", msgToSend.msg);
                    } catch (UnknownHostException e) {
                        Log.e("UnknownHostException", msgToSend.msg);
                    }catch (IOException e){
                        Log.e("IOException", msgToSend.msg);
                        e.printStackTrace();
                    } catch (Exception e){
                        Log.e("Exception", msgToSend.msg);
                    }
                }
            return null;
        }
    }

    public class SeqComparator implements Comparator<Message> {

        @Override
        public int compare(Message lhs, Message rhs) {
            if(lhs.seq < rhs.seq){
                return -1;
            }
            else{
                return 1;
            }
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
    boolean deliverable;
    int myPort;
    int proposedSeqNo = -1;
    int finalSeqNo = -1;
    int key=0;

    Message(String msg, int seq, int myPort){
        this.msg = msg;
        this.seq = seq;
        this.myPort = myPort;
    }
}
