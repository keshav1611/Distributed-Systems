package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static String myPort;
    static String myHash;
    static String portStr;
    static HashMap<String, String> successor = new HashMap<String, String>();
    static HashMap<String, String> predecessor = new HashMap<String, String>();
    static final int SERVER_PORT = 10000;
    TreeMap<String, String> avdDht = new TreeMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        DbHelper dbHelper = new DbHelper(getContext());
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        String[] splits = selection.split(":");
        selection = splits[0];
        Message del = new Message();
        del.action = "delete";
        del.port = successor.get("value");
        del.selection = selection;
        del.sender = myHash;
        if(splits.length > 1){
            del.sender = splits[1];
        }
        Log.v("Uridel",uri.toString());
        Log.v("Selectiondel",selection);

        db.execSQL("DELETE FROM "+ DbHelper.TABLE_NAME);
        if(selection.equals("*") && (!successor.get("key").equals(del.sender))){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,del);
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
        DbHelper dbHelper = new DbHelper(getContext());
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        try {
            String keyHash = genHash(key);
            String print = keyHash + " : " + key;
//            Log.v("MsgHash",print);

//            if(portStr != "5554"){
//                Message msg = new Message();
//                msg.action = "getneighbours";
//                msg.port = portStr;
//
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
//            }

            String s = predecessor.get("key") + ", predSize : " + predecessor.size();
//            Log.v("pred", s);
//            Log.v("Me", myHash);
//            Log.v("Succ", successor.get("key"));

            if(myHash.equals(predecessor.get("key")) && myHash.equals(successor.get("key"))){
                db.insertWithOnConflict(DbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
            }
            else if(predecessor.size() == 3 && (keyHash.compareTo(predecessor.get("key")) > 0 || keyHash.compareTo(myHash) == 0 || keyHash.compareTo(myHash) < 0)){
                db.insertWithOnConflict(DbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
            }
            else if(((keyHash.compareTo(myHash) == 0) || (keyHash.compareTo(myHash) < 0)) && (keyHash.compareTo(predecessor.get("key")) > 0)){
                db.insertWithOnConflict(DbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
            }
            else{
                Message sendToSuccessor = new Message();
                sendToSuccessor.action = "check";
                sendToSuccessor.port = successor.get("value");
                sendToSuccessor.key = key;
                sendToSuccessor.value = value;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendToSuccessor);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

//        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.v("Port",portStr);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            myHash = genHash(portStr);
            Log.v("postHash",myHash);
        } catch (NoSuchAlgorithmException e) {
            Log.e("Error","NoSuchAlgorithm");
            e.printStackTrace();
        }

        avdDht.put(myHash,portStr);
        successor.put("key",myHash);
        successor.put("value",avdDht.get(myHash));
        predecessor.put("key",myHash);
        predecessor.put("value",avdDht.get(myHash));

        if(portStr != "5554"){
            Message msg = new Message();
            msg.port = portStr;
            msg.action = "addme";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
        }
//        for(Map.Entry map : avdDht.entrySet()){
//            Log.v(map.getKey().toString(),map.getValue().toString());
//        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        DbHelper dbHelper = new DbHelper(getContext());
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        Cursor cursor = db.query(DbHelper.TABLE_NAME, null, null, null, null, null, null);
        String[] splits = selection.split(":");
        selection = splits[0];

        if(selection.equals("*")){
            Message query = new Message();
            query.selection = selection;
            query.action = "query";
            query.port = successor.get("value");
            query.sender = myHash;
            if(splits.length > 1){
                query.sender = splits[1];
            }
            if(!(myHash.equals(predecessor.get("key")) && myHash.equals(successor.get("key")))){
                MergeCursor mergeCursor;
                try {
                    if(!successor.get("key").equals(query.sender)){
                        Log.v("SendToNext",successor.get("value"));
                        Cursor res = new FindQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query).get();
                        mergeCursor = new MergeCursor(new Cursor[]{res, cursor});
                        mergeCursor.moveToFirst();
                        return mergeCursor;
                    }
                    else{
                        Log.v("Last",myPort);
                        cursor.moveToFirst();
                        return cursor;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            }
        }
        else if(selection.equals("@")){
            cursor = db.query(DbHelper.TABLE_NAME, null, null, null, null, null, null);
        }else{
            selectionArgs = new String[] {selection};
            selection = DbHelper.COLUMN_KEY + " = ?";
            cursor = db.query(DbHelper.TABLE_NAME, null, selection, selectionArgs, null, null, null);
//            Log.v("CursorCount",Integer.toString(cursor.getCount()));
//            Log.v("CurPos",Integer.toString(cursor.getPosition()));
            cursor.moveToFirst();
//            Log.v("CurFirstPos",Integer.toString(cursor.getPosition()));
            if(cursor.getCount() != 0){
//                Log.v("FoundKey",selectionArgs[0]);
                return cursor;
            }

            if(cursor.getCount() == 0){
                Message query = new Message();
                query.action = "query";
                query.selection = selectionArgs[0];
//                Log.v("Succcccc",successor.get("value"));
//                Log.v("Selection", query.selection);
                query.port = successor.get("value");

                try {
                    Cursor res = new FindQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query).get();
//                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
//                    matrixCursor.addRow(new Object[]{res[0], res[1]});
                    cursor = res;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
//        Log.v("CursorCount",Integer.toString(cursor.getCount()));

//        if(!selection.equals('*') && !selection.equals('@')){
//            selectionArgs = new String[] {selection};
//            Log.v("SeleArrgs",selectionArgs[0]);
//            selection = DbHelper.COLUMN_KEY + " = ?";
//
//            cursor = db.query(DbHelper.TABLE_NAME, null, selection, selectionArgs, null, null, null);
//        }

//        Log.v("CursorCount",Integer.toString(cursor.getColumnCount()));
//        Log.v("Cursor",Boolean.toString(cursor.isNull(0)));
//        Log.v("Cursor1",Boolean.toString(cursor.isNull(1)));
//        if (cursor.moveToFirst()){
//            do{
//                String data = cursor.getString(cursor.getColumnIndex("key"));
//                Log.v("Cursor",data);
//                // do what ever you want here
//            }while(cursor.moveToNext());
//        }

//        Log.v("ReturnPos",Integer.toString(cursor.getPosition()));
        cursor.moveToFirst();
        return cursor;
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

    private class ServerTask extends AsyncTask<ServerSocket,Void,Void>{

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];

            try{
                while(true){
                    Socket socket = serverSocket.accept();
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message message = (Message) ois.readObject();

                    if(message.action.equals("addme")){
                        String portToAddHash = genHash(message.port);
                        avdDht.put(portToAddHash, message.port);
                        String succkey = avdDht.higherKey(myHash);
                        String predkey = avdDht.lowerKey(myHash);
                        String highestkey = avdDht.lastKey();
                        String lowestkey = avdDht.firstKey();
                        successor.clear();
                        predecessor.clear();
                        if(succkey != null){
                            successor.put("key", succkey);
                            successor.put("value", avdDht.get(succkey));
                        }
                        else{
                            successor.put("key", lowestkey);
                            successor.put("value", avdDht.get(lowestkey));
                        }
                        if(predkey != null){
                            predecessor.put("key", predkey);
                            predecessor.put("value", avdDht.get(predkey));
                        }
                        else{
                            predecessor.put("key", highestkey);
                            predecessor.put("value", avdDht.get(highestkey));
                            predecessor.put("first","iamlowest");
                        }
                        oos.writeObject(avdDht);
                        oos.flush();
                    }
                    else if(message.action.equals("updatedht")){
                        avdDht = message.updatedDht;
                        String succkey = avdDht.higherKey(myHash);
                        String predkey = avdDht.lowerKey(myHash);
                        String highestkey = avdDht.lastKey();
                        String lowestkey = avdDht.firstKey();
                        successor.clear();
                        predecessor.clear();
                        if(succkey != null){
                            successor.put("key", succkey);
                            successor.put("value", avdDht.get(succkey));
                        }
                        else{
                            successor.put("key", lowestkey);
                            successor.put("value", avdDht.get(lowestkey));
                        }
                        if(predkey != null){
                            predecessor.put("key", predkey);
                            predecessor.put("value", avdDht.get(predkey));
                        }
                        else{
                            predecessor.put("key", highestkey);
                            predecessor.put("value", avdDht.get(highestkey));
                            predecessor.put("first","iamlowest");
                        }
                    }
                    else if(message.action.equals("query")){
                        if(message.sender != null){
                            message.selection = message.selection + ":" + message.sender;
                        }
                        Cursor cursor = query(Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider"),null, message.selection, null, null);
//                        if(cursor.getCount() != 0){
//                            Log.v("CursorKey",cursor.getString(cursor.getColumnIndex("key")));
//                        }
//                        else{
//                            Log.v("Cursor","Empty");
//                        }
                        HashMap cursorMap = new HashMap();
                        if(cursor.moveToFirst()){
                            do{
                                String cursorKey = cursor.getString(cursor.getColumnIndex("key"));
                                String cursorValue = cursor.getString(cursor.getColumnIndex("value"));
                                cursorMap.put(cursorKey,cursorValue);
                            }while (cursor.moveToNext());
                        }
//                        String[] cursorList = {cursorKey, cursorValue};
                        oos.writeObject(cursorMap);
                        oos.flush();
                    }
                    else if(message.action.equals("delete")){
                        message.selection = message.selection + ":" + message.sender;
                        delete(Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider"), message.selection, null);
                    }
//                    else if(message.action.equals("getneighbours")){
//                        Log.v("Entry","a");
//                        String portToFindHash = genHash(message.port);
//                        String succkey = avdDht.higherKey(portToFindHash);
//                        String predkey = avdDht.lowerKey(portToFindHash);
//                        String highestkey = avdDht.lastKey();
//                        String lowestkey = avdDht.firstKey();
//                        HashMap<String, String> successorSend = new HashMap<String, String>();
//                        HashMap<String, String> predecessorSend = new HashMap<String, String>();
//                        if(succkey != null){
//                            successorSend.put("key", succkey);
//                            successorSend.put("value", avdDht.get(succkey));
//                        }
//                        else{
//                            successorSend.put("key", lowestkey);
//                            successorSend.put("value", avdDht.get(lowestkey));
//                        }
//                        if(predkey != null){
//                            predecessorSend.put("key", predkey);
//                            predecessorSend.put("value", avdDht.get(predkey));
//                        }
//                        else{
//                            predecessorSend.put("key", highestkey);
//                            predecessorSend.put("value", avdDht.get(highestkey));
//                            predecessorSend.put("first","iamlowest");
//                        }
//                        ArrayList<HashMap> neighbours = new ArrayList<HashMap>();
//                        neighbours.add(successorSend);
//                        neighbours.add(predecessorSend);
////                        Log.v("Succ",successorSend.get("key"));
//                        Log.v("Neigh",predecessorSend.get("key"));
//                        oos.writeObject(neighbours);
//                        oos.flush();
//                    }
                    else if(message.action.equals("check")){
                        ContentValues values = new ContentValues();
                        values.put("key",message.key);
                        values.put("value",message.value);
                        insert(Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider"), values);
                    }

//                    for(Map.Entry map : avdDht.entrySet()){
//                        Log.v(map.getKey().toString(),map.getValue().toString());
//                    }

//                    oos.writeObject(avdDht);
//                    oos.flush();
//                oos.close();
//                ois.close();
//                socket.close();
                }
            }
            catch(Exception e){

            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<Message,Void,Void>{

        @Override
        protected Void doInBackground(Message... messages) {
            try {
                Message message = messages[0];

                if (message.action.equals("check") || message.action.equals("query") || message.action.equals("delete")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(message.port)*2);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

                    oos.writeObject(message);
                    oos.flush();
                }
                else {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11108);

                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

                oos.writeObject(message);
                oos.flush();

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

//                if (message.action.equals("getneighbours")) {
//                    ArrayList<HashMap> neighbours = new ArrayList<HashMap>();
//                    neighbours = (ArrayList) ois.readObject();
//
//                    successor = neighbours.get(0);
//                    predecessor = neighbours.get(1);
//
//                }
                if(message.action.equals("addme")){
                    TreeMap updatedDht = (TreeMap) ois.readObject();
                    Message tree = new Message();
                    tree.action = "updatedht";
                    tree.updatedDht = updatedDht;
                    int REMOTE_PORTS[] = {11112, 11116, 11120, 11124};

                    for(int i: REMOTE_PORTS){
                        Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i);
                        ObjectOutputStream oos1 = new ObjectOutputStream(socket1.getOutputStream());
                        oos1.writeObject(tree);
                        oos1.flush();
                    }
                }
//                TreeMap receivedDht = (TreeMap) ois.readObject();
//                avdDht = receivedDht;
//                String succkey = avdDht.ceilingKey(myHash);
//                String predkey = avdDht.floorKey(myHash);
//                if(succkey != null){
//                    successor.put("key", succkey);
//                    successor.put("value", avdDht.get(succkey));
//                }
//                else{
//                    successor.put("key", myHash);
//                    successor.put("value", avdDht.get(myHash));
//                }
//                if(predkey != null){
//                    predecessor.put("key", predkey);
//                    predecessor.put("value", avdDht.get(predkey));
//                }
//                else{
//                    predecessor.put("key", myHash);
//                    predecessor.put("value", avdDht.get(myHash));
//                }

//                for (Map.Entry map : avdDht.entrySet()) {
//                    Log.v(map.getKey().toString(), map.getValue().toString());
//                }

                oos.close();
//                ois.close();
                socket.close();
            }
            } catch (UnknownHostException e) {
                Log.e("Error", "Unknown");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e("Error", "IO");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class FindQuery extends AsyncTask<Message, Void, Cursor>{
        @Override
        protected Cursor doInBackground(Message... messages) {
            Message query = messages[0];
            HashMap cursorMap = new HashMap();
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(query.port)*2);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(query);
                oos.flush();

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                cursorMap = (HashMap) ois.readObject();
                for(Object i: cursorMap.keySet()){
                    matrixCursor.addRow(new Object[] {i, cursorMap.get(i)});
                }
//                matrixCursor.addRow(new Object[]{selection[0], selection[1]});
//                Log.v("GotKey",query.port);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            matrixCursor.moveToFirst();
            return matrixCursor;
        }
    }

    public class DbHelper extends SQLiteOpenHelper {

        public static final int DATABASE_VERSION = 1;
        public static final String DATABASE_NAME = "SimpleDht.db";
        public static final String TABLE_NAME = "messenger";
        public static final String COLUMN_KEY = "key";
        public static final String COLUMN_VALUE = "value";

        private static final String TABLE_ENTRY =
                "CREATE TABLE " + TABLE_NAME + " (" + COLUMN_KEY + " TEXT PRIMARY KEY, " + COLUMN_VALUE + " TEXT)";

        public DbHelper(Context context){
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(TABLE_ENTRY);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            onCreate(db);
        }
    }
}

class Message implements Serializable{
    private static final long serialVersionUID = -299482035708790407L;
    String port;
    String action;
    String key;
    String value;
    TreeMap<String, String> updatedDht;
    String selection;
    String sender = null;
}
