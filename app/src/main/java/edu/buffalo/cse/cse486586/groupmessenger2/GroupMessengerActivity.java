package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.concurrent.PriorityBlockingQueue;
import android.os.Handler;
import static java.lang.Math.max;

//https://studylib.net/doc/7830646/isis-algorithm-for-total-ordering-of-messages

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final int [] allPorts = new int[]{11108, 11112, 11116, 11120, 11124};
    static final List<Integer> allPortsList = new ArrayList<Integer>(Arrays.asList(11108, 11112, 11116, 11120, 11124));

    //From PA2A OnPTestClickListener.java File
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");;
    private ContentValues keyValueToInsert;
    private static int si = -1;
    private static int counteri = -1;
    private static int finalCounter = -1;
    private static int numberOfAvdsAlive = 5;
    private static final PriorityBlockingQueue<Message> holdBackQueue = new PriorityBlockingQueue<Message> ();
    private static final Hashtable<String, List<Sugseq>> sugSeqList = new Hashtable<String, List<Sugseq>> ();
    boolean occurOnce = false;
    boolean heartExcept = false;

    //From PA2A OnPTestClickListener.java File
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
        //From PA1
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d(TAG, "onCreate: "+myPort);
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.d(TAG, "onCreate: initialized server socket");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);//Creating the async task
            Log.d(TAG, "onCreate: initialized a new server task");
            //This will run in parallel - thread pool executer
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "onCreate: ", e);
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final TextView tv = (TextView) findViewById(R.id.textView1);
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
        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button sendButton = (Button) findViewById(R.id.button4);
        Log.d(TAG, "onCreate: before click listener");

        sendButton.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                if(!occurOnce) {
                    heartBeats(myPort);
                    occurOnce = true;
                }
                // Code here executes on main thread after user presses button
                String grpMsg = editText.getText().toString() + "\n";
                editText.setText("");
                Log.d("typed", "typed: "+grpMsg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, grpMsg, myPort, "bDel1");//Serializing async
            }
        });

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    //From PA1
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.d(TAG, "here in server task do in background");
            try {
                while(true) {
                    Socket socket = serverSocket.accept();
                    Log.d(TAG, "here1");
                    Message recieveInp = (Message) (new ObjectInputStream(socket.getInputStream())).readObject();
                    Log.d(TAG, "here2");
                    String recMsg = String.valueOf(recieveInp.getMsg());
                    Log.d(TAG, "message recieved: "+recMsg);
                    char msgType = recieveInp.getType().charAt(recieveInp.getType().length() - 1);
                    //https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data/5680427
                    switch(msgType)
                    {
                        case '1': // Initial message
                            si += 1;
                            new sendProposalTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                    recieveInp.getMsg(), recieveInp.getMsgId(), String.valueOf(si), String.valueOf(recieveInp.getPid()), String.valueOf(recieveInp.getTo()));//Serializing async
                            //      "the original message" "the original message id" "The proposal value" "the to address"      "the from address"
                            recieveInp.setSeqNo(si);
                            recieveInp.setStatus(0);
                            holdBackQueue.add(recieveInp);
                            break;
                        case '2': // Message with proposed sequence number
                            Log.d("processkval", "recinp.getPid: "+recieveInp.getPid());
                            Sugseq nSugSeqObj = new Sugseq(recieveInp.getSeqNo(), recieveInp.getPid());
                            boolean isDone = insertToSuggList(nSugSeqObj, recieveInp.getMsgId());
                            if(isDone){
                                Sugseq finalSeq = retrieveValidSeq(recieveInp.getMsgId());
                                sugSeqList.remove(recieveInp.getMsgId());
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                        recieveInp.getMsgId(), String.valueOf(recieveInp.getTo()), "bDel3",
                                        String.valueOf(finalSeq.getSeqNo()), String.valueOf(finalSeq.getProcessK()));//Serializing async
                            }
                            break;
                        case '3': // Message with agreed sequence number
                            si = max(si, recieveInp.getSeqNo());
                            Message finalMessage = getFinalMessageFromQueue(recieveInp.getMsgId(), recieveInp.getSeqNo(), recieveInp.getSuggProcK());
                            break;
                        default:
                            //No defaults
                            break;
                    }

                    socket.close();

                    //Keep checking the head of the priority queue for a valid message that can be delivered
                    //This is known when the head message has a status of 1
                    Message m = holdBackQueue.peek();
                    while(m != null && m.getStatus() == 1){
                        finalCounter += 1;
                        Log.d("headdel", "msg: " + m.getMsg() + " and Seqno: " + m.getSeqNo() + " and finalc: " + finalCounter + " and sugProcK: " + m.getSuggProcK());
                        publishProgress(m.getMsg(), String.valueOf(finalCounter));
                        holdBackQueue.poll();
                        m = holdBackQueue.peek();
                    }
                    //Check the hashtable of messages for proposal checks
                    checkMap();

                    Log.d("pqsize", "Size of priority queue: "+holdBackQueue.size());
                }
            } catch (IOException e) {
                Log.e(TAG, "IO Exception"+e);
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "classnotfound exception: "+e);
                e.printStackTrace();
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strReceived + "\n");
            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */
            setContentUsingProvider(strReceived, strings[1]);

            return;
        }
    }

    //From PA1
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            int currentPort = Integer.parseInt(msgs[1]);
            int nDevices = 0;
            String msgToSend = msgs[0];
            Message msgObject = null;

            if(msgs[2] == "bDel1") {
                counteri += 1;
                String msgToSendId = String.valueOf(currentPort) + String.valueOf(counteri);
                msgObject = new Message(msgToSend, currentPort, msgToSendId, msgs[2], counteri);
                Log.d("bDel1", "message: "+msgObject.getMsg()+", counter: "+msgObject.getCounter()+", msgid: "+msgObject.getMsgId());
            }
            else if(msgs[2] == "bDel3"){
                msgObject = new Message(msgToSend, currentPort, msgToSend, msgs[2], Integer.parseInt(msgs[4]));
                msgObject.setSeqNo(Integer.parseInt(msgs[3]));
            }
            else if(msgs[2] == "heartBeat"){
                msgObject = new Message(currentPort, msgs[2], "false");
            }
            //Run a loop for five devices and create a socket to connect to other devices except the current device
            while(nDevices < numberOfAvdsAlive){
                int remotePort = allPortsList.get(nDevices);
                try {
                    //From PA1
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            remotePort);
                    msgObject.setTo(remotePort);

                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */
                    //https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data
                    //Log.d("debug", "Message : "+msgToSend+", isEmpty :"+msgToSend.trim().isEmpty());
                    OutputStream msgOpStrm = socket.getOutputStream();
                    ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
                    sendOp.writeObject(msgObject);
                    sendOp.flush();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    if(msgObject.getType() == "heartBeat"){
                        Log.w("NodeFailure", "port : "+msgObject.getTo());
                        numberOfAvdsAlive = 4;
                        if(allPortsList.contains(msgObject.getTo()) && heartExcept == false){
                            heartExcept = true;
                            int index = allPortsList.indexOf(msgObject.getTo());
                            allPortsList.remove(index);
                            Iterator itr = holdBackQueue.iterator();
                            while(itr.hasNext()){
                                Message m = (Message) itr.next();
                                Log.w("NodeFailure", "ispresent: "+allPortsList.contains(m.getPid())+", msgid"+m.getMsgId());
                                if(!allPortsList.contains(m.getPid()) && m.getStatus() == 0) {
                                    Log.w("NodeFailure", "removing message from queue");
                                    holdBackQueue.remove(m);
                                }
                            }
                        }
                    }
                    Log.e(TAG, "ClientTask socket IOException"+e);
                }
                nDevices+=1;
            }
            return null;
        }
    }

    //The function is used to send the proposal message back to the process from which the message was sent.
    private class sendProposalTask extends AsyncTask<String, Void, Void> {
        //recieveInp.getMsg(),      recieveInp.getMsgId(),      String.valueOf(si),     String.valueOf(recieveInp.getPid()),    String.valueOf(recieveInp.getTo()))
        //"the original message"    "the original message id"   "The proposal value"    "the to address"                        "the from address"
        @Override
        protected Void doInBackground(String ...msgs) {
            String msgId = msgs[1];
            int seqNo = Integer.parseInt(msgs[2]);
            Message msgObject = new Message(msgs[0], Integer.parseInt(msgs[4]), msgId, "bDel2", seqNo);
            try {
                Socket psock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[3]));
                msgObject.setTo(Integer.parseInt(msgs[3]));
                OutputStream msgOpStrm = psock.getOutputStream();
                ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
                sendOp.writeObject(msgObject);
                sendOp.flush();
                psock.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException"+e);
            }
            return null;
        }
    }

    //This function helps in inserting the message's proposal into a hash table
    //The function returns true if all the proposals are received for a particular message
    //Else returns false
    public boolean insertToSuggList(Sugseq s, String msgId){
        if(sugSeqList.containsKey(msgId)){
            List<Sugseq> sList = sugSeqList.get(msgId);
            sList.add(s);
            Log.d("anodes", "alive nodes: "+numberOfAvdsAlive);
            if(sList.size() == numberOfAvdsAlive){//Check if all proposals are received from the group
                return true;
            }
        }
        else{
            List<Sugseq> sList = new ArrayList<Sugseq>();
            sList.add(s);
            sugSeqList.put(msgId, sList);
        }
        return false;
    }

    //Retrieve the final proposed sequence number for a message
    //This is at the head of the list after sorting the list
    public Sugseq retrieveValidSeq(String msgId){
        if(sugSeqList.containsKey(msgId)){
            List<Sugseq> sList = sugSeqList.get(msgId);
            Collections.sort(sList);
            Log.d("slist", "0 : "+sList.get(0).getSeqNo()+"#"+sList.get(0).getProcessK()+", 1 : "+sList.get(1).getSeqNo()+"#"+sList.get(1).getProcessK());
            return sList.get(0);
        }
        return null;
    }

    //This function helps in updating the message's sequence number, status, and the process that
    // suggested the sequence number and reorder the priority queue
    public Message getFinalMessageFromQueue(String msgId, int finSeq, int sugPk){
        Iterator itr = holdBackQueue.iterator();
        Message m = null;
        while (itr.hasNext()) {
            m = (Message) itr.next();
            if(Integer.parseInt(m.getMsgId()) == Integer.parseInt(msgId)) {
                break;
            }
        }
        if(m != null){
            holdBackQueue.remove(m);
            m.setStatus(1);
            m.setSeqNo(finSeq);
            m.setKey(sugPk);
            m.setSuggProcK(sugPk);
            holdBackQueue.add(m);
            return m;
        }
        return null;
    }

    //This function checks the hashtable if it has any message ids exist with all the proposals
    //received but the valid final proposal is not chosen
    public synchronized void checkMap(){
        Iterator sitr = sugSeqList.entrySet().iterator();
        while (sitr.hasNext()) {
            Map.Entry<String, List> entry = (Map.Entry<String, List>) sitr.next();
            String key = entry.getKey();
            Log.d("sugseqlist", "printMap: msgid" + key + ", length: " + sugSeqList.get(key).size());
            if(sugSeqList.get(key).size() == numberOfAvdsAlive){
                String to = key.substring(0,5);//Get to address from message id (portnumber(0-5)+counter)
                Sugseq finalSeq = retrieveValidSeq(key);//Get the final proposed sequence from the list
                sitr.remove();//Remove the msg from the hashtable
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        key, String.valueOf(to), "bDel3",
                        String.valueOf(finalSeq.getSeqNo()), String.valueOf(finalSeq.getProcessK()));//Serializing async
            }
        }
    }

    public void setContentUsingProvider(String msg, String key){
        try {
            keyValueToInsert = new ContentValues();
            //From Documentation
            keyValueToInsert.put(KEY_FIELD, key);
            keyValueToInsert.put(VALUE_FIELD, msg);
            Uri newUri = getContentResolver().insert(
                    mUri,    // assume we already created a Uri object with our provider URI
                    keyValueToInsert
            );
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }
    }

    //https://stackoverflow.com/questions/13422818/how-can-i-invoke-a-method-every-5-seconds-in-android/13423036
    //The function keeps getting executed for every 1second and sends a heart beat to all the nodes in the network group
    private final int timeout = 1000;
    public void heartBeats(final String currentPort) {
        final Handler handler = new Handler();
        handler.postDelayed(new Runnable() {
            public void run() {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "false", currentPort, "heartBeat");
                handler.postDelayed(this, timeout);
            }
        }, timeout);
    }


}

//Class to send the message as an object during multicast
class Message implements Comparable<Message>, Serializable {

    private String msg;//The message that the process wants to send
    private int pid;//The id of the process that sends the message
    private String type;//The type of the message 1. bDel1 : first bmult, 2. bDel2 : proposal msg, 3. final msg bmult
    private String msgId;//A unique id of the message : portnumber+counter
    private int counter;//Number of messages sent
    private int seqNo;//The sequence number for ensuring total ordering
    private int key;//The key value to be inserted
    private int status;//Deliverable - 1 or Undeliverable - 0
    private int to;//The remote destination address port number
    private int suggProcK;//The processor that suggested the sequence number

    public Message(String msg, int processId, String msgId, String type, int... k){
        this.msg = msg;
        this.pid = processId;
        this.type = type;
        this.msgId = msgId;
        this.status = 0;
        if(type == "bDel1"){//This is the first try message to be sent by client
            this.counter = k[0];
        }
        else if(type == "bDel2"){//This is the message sent after retrieving all proposals
            this.seqNo = k[0];
        }
        else if(type == "bDel3"){//This is the message that will be delivered
            this.key = k[0];
            this.suggProcK = k[0];
        }
    }

    public Message(int fromVal, String typeVal, String msg){
        this.pid = fromVal;
        this.type = typeVal;
        this.msg = msg;
    }

    //Getter and Setter methods

    public String getMsg(){
        return this.msg;
    }

    public int getPid(){
        return this.pid;
    }

    public String getType(){
        return this.type;
    }

    public String getMsgId(){
        return this.msgId;
    }

    public int getCounter(){
        return this.counter;
    }

    public int getSeqNo(){
        return this.seqNo;
    }

    public int getKey(){
        return this.key;
    }

    public int getStatus(){
        return this.status;
    }

    public int getTo(){
        return this.to;
    }

    public int getSuggProcK(){
        return this.suggProcK;
    }

    public void setTo(int toVal){
        this.to = toVal;
    }

    public void setSeqNo(int seqVal){
        this.seqNo = seqVal;
    }

    public void setStatus(int statusVal){
        this.status = statusVal;
    }

    public void setKey(int keyVal){
        this.key = keyVal;
    }

    public void setSuggProcK(int pk){
        this.suggProcK = pk;
    }


    //The message object is added to a priority queue and the ordering is done as follows
    @Override
    public int compareTo(Message m) {
        //Sort such that message with smallest sequence number is at the head
        if(this.seqNo < m.seqNo)
            return -1;
        else if(this.seqNo > m.seqNo)
            return 1;
        else if(this.seqNo == m.seqNo){//Tie break
            //If two sequence numbers are the same then place any undeliverable messages at the head
            if(this.status < m.status)
                return -1;
            else if(this.status > m.status)
                return 1;
            else{
                //to break further ties, place message with smallest suggesting process id at the head
                if(this.suggProcK < m.suggProcK)
                    return -1;
                else if(this.suggProcK > m.suggProcK)
                    return 1;
                else
                    return 0;
            }
        }
        else
            return 0;
    }
}


/*Class to send the suggested sequence number as an object
The object created contains a sequence number and the id of the
processor that proposed that particular sequence number*/
class Sugseq implements Comparable<Sugseq> {

    private int seqNo;
    private int processK;

    public Sugseq(int seqVal, int pk){
        this.seqNo = seqVal;
        this.processK = pk;
    }

    //Getter methods
    public int getSeqNo(){
        return this.seqNo;
    }

    public int getProcessK(){
        return this.processK;
    }

    //The object is added to a list which is sorted in the following order
    @Override
    public int compareTo(Sugseq s) {
        //Sort such that sequence with largest sequence number is at the head of the list
        if(this.seqNo < s.seqNo)
            return 1;
        else if(this.seqNo > s.seqNo)
            return -1;
        else if(this.seqNo == s.seqNo){//Tie breaker
            //If two sequence numbers are the same then place sequence with smallest process id
            // at the head of the list
            if(this.processK < s.processK)
                return -1;
            else if(this.processK > s.processK)
                return 1;
            else
                return 0;
        }
        else
            return 0;
    }
}
