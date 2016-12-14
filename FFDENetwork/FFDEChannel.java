package FFDENetwork;

import java.io.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by JM on 25.03.2016.
 * This object represents a communication channel being a wrapper for a network socket (from java.net package)
 * This allows to perform simplified interaction with the socket without much coding in other modules of FFDE package.
 */
public class FFDEChannel implements FFDEObserver {

    private Scanner rx;                         //< receiver
    private final Object rxLock = new Object(); //< receiver synchronization lock

    private BufferedWriter tx;                  //< transmitter
    private final Object txLock = new Object(); //< transmitter synchronization lock

    private Queue<String> rxBuffer = new ConcurrentLinkedQueue<>();

    private volatile String name = "";              //< channel identifier (should indicate destination)
    private Socket socket = null;                   //< the parent socket of the channel (NOT THREAD-SAFE!)
    private FFDETimer timer;                        //< timer used for updating the channel
    private FFDEEventDispatcher eventDispatcher;    //< dispatcher responsible for collecting events from the channel
    private String rxEventID;                       //< identifier of rx events from this channel

    // ++++++++++++++ CONSTRUCTORS ++++++++++++++

    /**
     * Creates a new instance of <code>FFDECHannel</code>. Used to encapsulate <I>Java.net.socket</I> object.
     * @param aName         identifier of the channel
     * @param aSocket       reference to the socket encapsulated by this channel
     * @param aDispatcher   reference to the dispatcher collecting events from this channel
     * @param aTimer        reference to the timer responsible for updating the channel
     * @param aEventID      string used as an identifier for rx events coming from this channel
     */
    public FFDEChannel(String aName, Socket aSocket, FFDEEventDispatcher aDispatcher, FFDETimer aTimer,
                       String aEventID) {
        name = aName;
        socket = aSocket;
        timer = aTimer;
        eventDispatcher = aDispatcher;
        rxEventID = aEventID;
        // extract IO streams from the socket
        try{
            //synchronized (rxLock) {
            rx = new Scanner(new InputStreamReader(socket.getInputStream()));
            //}
            //synchronized (txLock) {
            tx = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            //}
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        timer.observeFFDE(this, "tick");       //< start observing the timer used for sending updates to the channel

        Thread rxUpdaterThread = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (rxLock) {
                    while(true) {
                        String newLine = rx.nextLine();
                        rxBuffer.add(newLine);
                    }
                }
            }
        });
        rxUpdaterThread.start();

        /*
        Thread rxUpdaterThread = new Thread(new RxScanner());
        rxUpdaterThread.setName("rxUpdater_" + name);
        rxUpdaterThread.start();
        */
    }

    /**
     * Creates a new instance of <code>FFDECHannel</code>. Used to encapsulate <I>Java.net.socket</I> object.
     * @param aSocket       reference to the socket encapsulated by this channel
     * @param aDispatcher   reference to the dispatcher collecting events from this channel
     * @param aTimer        reference to the timer responsible for updating the channel
     * @param aEventID      string used as an identifier for rx events coming from this channel
     */
    public FFDEChannel(Socket aSocket, FFDEEventDispatcher aDispatcher, FFDETimer aTimer,
                       String aEventID) {
        socket = aSocket;
        timer = aTimer;
        eventDispatcher = aDispatcher;
        rxEventID = aEventID;
        // extract IO streams from the socket
        try{
            //synchronized (rxLock) {
            rx = new Scanner(new InputStreamReader(socket.getInputStream()));
            //}
            //synchronized (txLock) {
            tx = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            //}
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        timer.observeFFDE(this, "tick");       //< start observing the timer used for sending updates to the channel

        Thread rxUpdaterThread = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (rxLock) {
                    while(true) {
                        String newLine = rx.nextLine();
                        rxBuffer.add(newLine);
                    }
                }
            }
        });
        rxUpdaterThread.start();
        /*
        Thread rxUpdaterThread = new Thread(new RxScanner());
        rxUpdaterThread.setName("rxUpdater_no_name");
        rxUpdaterThread.start();
        */
    }

    // ++++++++++++++ END OF CONSTRUCTORS ++++++++++++++

    /** TODO
     * Overrides method from <code>FFDENetwork.FFDEObserver</code> interface. This method is called on timer tick.
     * @param aEvent    tick event, carries no data so it is unimportant
     */
    @Override
    public void notifyFFDE(FFDEEvent aEvent) {
        // receive data (and push an rx event to the queue)

        // if the RX buffer contains messages report all of them in separate rx events
        if(rxBuffer.contains("#")) {  //< if any complete message was received
            List<String> message = new LinkedList<>();      //< container for the message
            String line = "";                         //< container for a single line (used for iterating until "#")
            // if more than one messages are in rxBuffer they get reported in separate events
            do {
                line = rxBuffer.remove();

                if (!line.equals("#")) {     //< do not put "#" into the event message
                    message.add(line);
                }
            }
            while (!line.equals("#"));

            eventDispatcher.reportEvent(new FFDEEvent(rxEventID, message));     //< report a new event to dispatcher
        }
    }

    /**
     * Puts data in the transmission buffer of the channel. Then data is transmitted automatically. (This method assumes
     * that a full message is loaded so it automatically adds '#' at the end)
     * @param aData     data to be transmitted
     */
    public void transmitMessage(List<String> aData) {
        synchronized (txLock) {
            // data transmission
            for (String s : aData) {
                try {
                    tx.write(s);
                    tx.newLine();       //< separates records of data in a message
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // ending message and flushing the buffer
            try {
                tx.write("#");
                tx.newLine();       //< separates records of data in a message
                tx.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Changes identifier got by generated rx events. DANGEROUS METHOD!
     * @param aNewID    new id
     */
    public void setRxIdentifier(String aNewID) {
        rxEventID = aNewID;
    }

    /**
     * Returns name of the channel.
     * @return String identifier/name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name of the channel.
     * @param aName     new name
     */
    public void setName(String aName) {
        name = aName;
    }

    /** TODO
     * <H1>DON'T USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.</H1>
     * This method returns first message from rx buffer of the channel. This should be done only in a few situations
     * when event distribution system is disabled and <code>notifyFFDE</code> can not be called by a timer.
     * @return      data from rx buffer
     */
    public List<String> getRxBuffer() {
            List<String> message = new LinkedList<>();      //< container for the message
            String line = "";                         //< container for a single line (used for iterating until "#")
            // if more than one messages are in rxBuffer they get reported in separate events
            do {
                line = rxBuffer.remove();

                if (!line.equals("#")) {     //< do not put "#" into the event message
                    message.add(line);
                }
            }
            while (!line.equals("#"));
        return message;
    }

    /**
     * Utility class responsible for re-writing everything that appears on the channels input to its buffer.
     */
    private class RxScanner implements Runnable {

        @Override
        public void run() {
            synchronized (rxLock) {
                while(true) {
                    String newLine = rx.nextLine();
                    rxBuffer.add(newLine);
                }
            }
        }
    }
}
