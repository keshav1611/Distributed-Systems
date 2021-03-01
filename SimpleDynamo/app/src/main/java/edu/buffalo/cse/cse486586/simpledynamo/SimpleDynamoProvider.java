package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
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
import android.widget.SeekBar;

public class SimpleDynamoProvider extends ContentProvider {
	TreeMap<String, Integer> avdDht = new TreeMap<String, Integer>();
	static String portStr;
	static String myHash;
	static final Integer[] PORT_NAMES = {5554, 5556, 5558, 5560, 5562};
	static final Integer[] REMOTE_PORTS = {11108, 11112, 11116, 11120, 11124};
	static final int SERVER_PORT = 10000;
	String[] columns = new String[]{DbHelper.COLUMN_KEY, DbHelper.COLUMN_VALUE};

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		DbHelper dbHelper = new DbHelper(getContext());
		SQLiteDatabase db = dbHelper.getWritableDatabase();
		db.execSQL("DELETE FROM "+ DbHelper.TABLE_NAME);

		ArrayList<Integer> ports = new ArrayList<Integer>(Arrays.asList(PORT_NAMES));
		ports.remove(ports.indexOf(Integer.parseInt(portStr)));

		Message delete = new Message();
		delete.action = "delete";
		delete.ports = ports;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delete);
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
		String key = values.getAsString(DbHelper.COLUMN_KEY);
		String value = values.getAsString(DbHelper.COLUMN_VALUE);
		String correctWriteNode;
		String replica1;
		String replica2;
		Message message = new Message();
		int version = 1;
//		for(Map.Entry map : avdDht.entrySet()){
//			Log.v(map.getKey().toString(),map.getValue().toString());
//		}

		try {
			String keyHash = genHash(key);
			Log.v(key,keyHash);
			if(avdDht.ceilingKey(keyHash) == null){
				correctWriteNode = avdDht.firstKey();
			}else{
				correctWriteNode = avdDht.ceilingKey(keyHash);
			}
			if(avdDht.higherKey(correctWriteNode) == null){
				replica1 = avdDht.firstKey();
			}else{
				replica1 = avdDht.higherKey(correctWriteNode);
			}
			if(avdDht.higherKey(replica1) == null){
				replica2 = avdDht.firstKey();
			}else{
				replica2 = avdDht.higherKey(replica1);
			}
			Log.v("correctWriteNode",correctWriteNode);
			Log.v("replica1",replica1);
			Log.v("replica2",replica2);
			Log.v("Port", Integer.toString(avdDht.get(correctWriteNode)));
			message.ports.add(avdDht.get(correctWriteNode));
			message.ports.add(avdDht.get(replica1));
			message.ports.add(avdDht.get(replica2));
			if(correctWriteNode.equals(myHash) || replica1.equals(myHash) || replica2.equals(myHash)){
				String selection = DbHelper.COLUMN_KEY + " = ?";
				String[] selectionArgs = new String[]{key};
				Cursor cursor = db.query(DbHelper.TABLE_NAME, null, selection, selectionArgs, null, null, null);
				if(cursor.getCount() != 0){
					cursor.moveToFirst();
					version = cursor.getInt(cursor.getColumnIndex(DbHelper.COLUMN_VERSION)) + 1;
				}
				values.put(DbHelper.COLUMN_VERSION,version);
				db.insertWithOnConflict(DbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
				message.ports.remove(message.ports.indexOf(avdDht.get(myHash)));
			}
			message.action = "insert";
			message.key = key;
			message.value = value;

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		try {
			myHash = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		for(int i: PORT_NAMES) {
			try {
				String hashValue = genHash(Integer.toString(i));
				avdDht.put(hashValue, i);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e("onCreate","IO");
			e.printStackTrace();
		}

		DbHelper dbHelper = new DbHelper(getContext());
		SQLiteDatabase db = dbHelper.getReadableDatabase();
//		Cursor cursor = db.query(DbHelper.TABLE_NAME, null, null, null, null, null, null);
//		if(cursor.moveToFirst()){
//			do{
//				String key = cursor.getString(cursor.getColumnIndex(DbHelper.COLUMN_KEY));
//				int version = cursor.getInt(cursor.getColumnIndex(DbHelper.COLUMN_VERSION));
//				Message reconcile = new Message();
//				reconcile.action = "reconcile";
//				reconcile.selection = key;
//				String succ, pred;
//				if(avdDht.higherKey(myHash) == null){
//					succ = avdDht.firstKey();
//				}else{
//					succ = avdDht.higherKey(myHash);
//				}
//				if(avdDht.lowerKey(myHash) == null){
//					pred = avdDht.lastKey();
//				}else{
//					pred = avdDht.lowerKey(myHash);
//				}
//				int[] ports = new int[]{avdDht.get(succ), avdDht.get(pred)};
//				for(int i: ports){
//					reconcile.port = i;
//					try {
//						HashMap reconcileMap = new Reconcile().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reconcile).get();
//						if(!reconcileMap.isEmpty()){
//							int receivedVersion = Integer.parseInt(reconcileMap.get(DbHelper.COLUMN_VERSION).toString());
//							String newKey = reconcileMap.get(DbHelper.COLUMN_KEY).toString();
//							String newValue = reconcileMap.get(DbHelper.COLUMN_VALUE).toString();
//							if(receivedVersion > version){
//								ContentValues values = new ContentValues();
//								values.put(DbHelper.COLUMN_KEY, newKey);
//								values.put(DbHelper.COLUMN_VALUE, newValue);
//								values.put(DbHelper.COLUMN_VERSION, receivedVersion);
//								db.insertWithOnConflict(DbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
//							}
//						}
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					} catch (ExecutionException e) {
//						e.printStackTrace();
//					}
//				}
//			}while (cursor.moveToNext());
//		}

		String succ, pred;
		if(avdDht.higherKey(myHash) == null){
				succ = avdDht.firstKey();
			}else{
				succ = avdDht.higherKey(myHash);
			}
			if(avdDht.lowerKey(myHash) == null){
				pred = avdDht.lastKey();
			}else{
				pred = avdDht.lowerKey(myHash);
			}
			int[] ports = new int[]{avdDht.get(succ), avdDht.get(pred)};
			HashMap[] maps = new HashMap[2];
			int j=0;
			for(int i: ports){
				Message reconcile = new Message();
				reconcile.port = i;
				reconcile.action = "reconcile";
				reconcile.selection = "@";
				try {
					maps[j] = new Reconcile().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reconcile).get();
					j++;
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
			HashMap map = new HashMap(maps[0]);
			map.putAll(maps[1]);
			for(Object i: map.keySet()){
				try {
					String haskKey = genHash(i.toString());
					String correctNode, replica1, replica2;
					if(avdDht.ceilingKey(haskKey) == null){
						correctNode = avdDht.firstKey();
					}else{
						correctNode = avdDht.ceilingKey(haskKey);
					}
					if(avdDht.higherKey(correctNode) == null){
						replica1 = avdDht.firstKey();
					}else{
						replica1 = avdDht.higherKey(correctNode);
					}
					if(avdDht.higherKey(replica1) == null){
						replica2 = avdDht.firstKey();
					}else{
						replica2 = avdDht.higherKey(replica1);
					}

					if(correctNode.equals(myHash) || replica1.equals(myHash) || replica2.equals(myHash)){
						ContentValues values = new ContentValues();
						values.put(DbHelper.COLUMN_KEY,i.toString());
						values.put(DbHelper.COLUMN_VALUE,map.get(i).toString());
						db.insertWithOnConflict(DbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
					}
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		DbHelper dbHelper = new DbHelper(getContext());
		SQLiteDatabase db = dbHelper.getReadableDatabase();
		Cursor cursor = db.query(DbHelper.TABLE_NAME, columns, null, null, null, null, null);

		if(selection.equals("@")){
			cursor = db.query(DbHelper.TABLE_NAME, columns, null, null, null, null, null);
		}
		else if(selection.equals("*")){
			Message query = new Message();
			query.action = "query";
			query.selection = selection;
			ArrayList<Integer> ports = new ArrayList<Integer>(Arrays.asList(PORT_NAMES));
			ports.remove(ports.indexOf(Integer.parseInt(portStr)));
			query.ports.addAll(ports);
			MergeCursor mergeCursor;
			try {
				Cursor res = new FindQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query).get();
				mergeCursor = new MergeCursor(new Cursor[]{cursor, res});
				return mergeCursor;
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		else{
			Message query = new Message();
			try {
				String keyHash = genHash(selection);
				String correctReadNode, replica1, replica2;
				if(avdDht.ceilingKey(keyHash) == null){
					correctReadNode = avdDht.firstKey();
				}else{
					correctReadNode = avdDht.ceilingKey(keyHash);
				}
				if(avdDht.higherKey(correctReadNode) == null){
					replica1 = avdDht.firstKey();
				}else{
					replica1 = avdDht.higherKey(correctReadNode);
				}
				if(avdDht.higherKey(replica1) == null){
					replica2 = avdDht.firstKey();
				}else{
					replica2 = avdDht.higherKey(replica1);
				}

				query.action = "query";
				query.ports.add(avdDht.get(correctReadNode));
				query.ports.add(avdDht.get(replica1));
				query.ports.add(avdDht.get(replica2));
				query.selection = selection;
				if(correctReadNode.equals(myHash) || replica1.equals(myHash) || replica2.equals(myHash)){
					selectionArgs = new String[]{selection};
					selection = DbHelper.COLUMN_KEY + " = ?";
					cursor = db.query(DbHelper.TABLE_NAME, columns, selection, selectionArgs, null, null, null);
					if(cursor.getCount() != 0){
						return cursor;
					}else{
						query.ports.remove(query.ports.indexOf(avdDht.get(myHash)));
					}
				}
				Log.v("Query",query.selection);
				cursor = new FindQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query).get();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		return cursor;
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

    private class ServerTask extends AsyncTask<ServerSocket, Void, Void>{
		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			try {
				while (true){
					Socket socket = serverSocket.accept();
					ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Message message = (Message) ois.readObject();

					if(message.action.equals("insert")){
						Log.v("Insert received",message.key + " : " + message.value);
						DbHelper dbHelper = new DbHelper(getContext());
						SQLiteDatabase db = dbHelper.getWritableDatabase();
						ContentValues values = new ContentValues();
						values.put("key",message.key);
						values.put("value",message.value);
//						Log.v("Inserted","True");
						int version = 1;
						String selection = DbHelper.COLUMN_KEY + " = ?";
						String[] selectionArgs = new String[]{message.key};
						Cursor cursor = db.query(DbHelper.TABLE_NAME, null, selection, selectionArgs, null, null, null);
						if(cursor.getCount() != 0){
							cursor.moveToFirst();
							version = cursor.getInt(cursor.getColumnIndex(DbHelper.COLUMN_VERSION)) + 1;
						}
						values.put(DbHelper.COLUMN_VERSION,version);
						db.insertWithOnConflict(DbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
					}
					else if(message.action.equals("query")){
						Log.v("Query received",message.selection);
						DbHelper dbHelper = new DbHelper(getContext());
						SQLiteDatabase db = dbHelper.getReadableDatabase();
						String[] selectionArgs = new String[]{message.selection};
						String selection = DbHelper.COLUMN_KEY + " = ?";
						Cursor cursor = db.query(DbHelper.TABLE_NAME, columns, selection, selectionArgs, null, null, null);
						if(message.selection.equals("*")){
							cursor = db.query(DbHelper.TABLE_NAME, columns, null, null, null, null, null);
						}
						HashMap cursorMap = new HashMap();
						if(cursor.moveToFirst()){
							do{
								String cursorKey = cursor.getString(cursor.getColumnIndex("key"));
								String cursorValue = cursor.getString(cursor.getColumnIndex("value"));
								cursorMap.put(cursorKey,cursorValue);
							}while (cursor.moveToNext());
						}
						oos.writeObject(cursorMap);
						oos.flush();
					}
					else if(message.action.equals("reconcile")){
						DbHelper dbHelper = new DbHelper(getContext());
						SQLiteDatabase db = dbHelper.getReadableDatabase();
//						String[] selectionArgs = new String[]{message.selection};
//						String selection = DbHelper.COLUMN_KEY + " = ?";
						Cursor cursor = db.query(DbHelper.TABLE_NAME, null, null, null, null, null, null);
						HashMap cursorMap = new HashMap();
						if(cursor.moveToFirst()){
							do{
								String cursorKey = cursor.getString(cursor.getColumnIndex("key"));
								String cursorValue = cursor.getString(cursor.getColumnIndex("value"));
								cursorMap.put(cursorKey,cursorValue);
							}while (cursor.moveToNext());
						}
						oos.writeObject(cursorMap);
						oos.flush();
					}
					else if(message.action.equals("delete")){
						DbHelper dbHelper = new DbHelper(getContext());
						SQLiteDatabase db = dbHelper.getWritableDatabase();
						db.execSQL("DELETE FROM "+ DbHelper.TABLE_NAME);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<Message, Void, Void>{
		@Override
		protected Void doInBackground(Message... messages) {
			Message message = messages[0];
			for (int i: message.ports){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i*2);
//					socket.setSoTimeout(2000);
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					oos.writeObject(message);
					oos.flush();
				}catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return null;
		}
	}

	private class FindQuery extends AsyncTask<Message, Void, Cursor>{
		@Override
		protected Cursor doInBackground(Message... messages) {
			Message message = messages[0];
			HashMap cursorMap;
			MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
			for (int i: message.ports){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i*2);
//					socket.setSoTimeout(2000);
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
					oos.writeObject(message);
					oos.flush();

					cursorMap = (HashMap) ois.readObject();
					for(Object j: cursorMap.keySet()){
						Log.v("Query returned",j.toString());
						matrixCursor.addRow(new Object[] {j, cursorMap.get(j)});
					}

					if(!message.selection.equals("*") && !cursorMap.isEmpty()){
						return matrixCursor;
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					Log.e("FindQuery","IO");
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
			matrixCursor.moveToFirst();
			return matrixCursor;
		}
	}

	private class Reconcile extends AsyncTask<Message, Void, HashMap>{
		@Override
		protected HashMap doInBackground(Message... messages) {
			Message message = messages[0];
			HashMap reconcileMap = new HashMap();
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), message.port*2);
//				socket.setSoTimeout(2000);
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				oos.writeObject(message);
				oos.flush();

				reconcileMap = (HashMap) ois.readObject();

			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (StreamCorruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return reconcileMap;
		}
	}

	public class DbHelper extends SQLiteOpenHelper {

		public static final int DATABASE_VERSION = 1;
		public static final String DATABASE_NAME = "SimpleDynamo.db";
		public static final String TABLE_NAME = "dynamo";
		public static final String COLUMN_KEY = "key";
		public static final String COLUMN_VALUE = "value";
		public static final String COLUMN_VERSION = "version";

		private static final String TABLE_ENTRY =
				"CREATE TABLE " + TABLE_NAME + " (" + COLUMN_KEY + " TEXT PRIMARY KEY, " + COLUMN_VALUE + " TEXT, " + COLUMN_VERSION + " INTEGER)";

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

class Message implements Serializable {
	private static final long serialVersionUID = -299482035708790407L;
	ArrayList<Integer> ports = new ArrayList<Integer>();
	String action;
	String key;
	String value;
	TreeMap<String, String> updatedDht;
	String selection;
	int port;
}