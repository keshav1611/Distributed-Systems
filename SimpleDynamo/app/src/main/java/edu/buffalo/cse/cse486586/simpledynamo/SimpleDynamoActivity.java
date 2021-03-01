package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		final TextView tv = (TextView) findViewById(R.id.textView1);
		final EditText editText1 = (EditText) findViewById(R.id.editText1);
		final EditText editText = (EditText) findViewById(R.id.editText);
		final Button insertButton = (Button) findViewById(R.id.button4);
		final Button ldump = (Button) findViewById(R.id.button1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		insertButton.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				String key = editText.getText().toString();
				String value = editText1.getText().toString();
				editText.setText("");
				editText1.setText("");

				ContentValues valuesToInsert = new ContentValues();
				valuesToInsert.put("key", key);
				valuesToInsert.put("value", value);

				getContentResolver().insert(Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider"),valuesToInsert);
			}
		});

		ldump.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				tv.setText("");
				Cursor cursor = getContentResolver().query(Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider"),null,"@",null,null);

				if(cursor.moveToFirst()){
					do{
						tv.append(cursor.getString(cursor.getColumnIndex("key")) + " : " + cursor.getString(cursor.getColumnIndex("value")) + "\n");
					}while (cursor.moveToNext());
				}
			}
		});
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
