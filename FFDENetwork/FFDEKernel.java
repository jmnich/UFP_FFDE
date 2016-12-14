package FFDENetwork;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Started by JM on 25.03.2016.
 * Finished by JM on 15.05.2016.
 * Finished by JM on TODO
 */
public class FFDEKernel implements FFDEObserver{

    // CONFIG
    private final int registrationDelay = 500;                          //< delay on registration state in [ms]
    private final int declarationDelay = 100;                           //< delay on declaration state in [ms]
    private final int subscriptionDelay = 100;                          //< delay on subscription state in [ms]

    // ports from this range are distributed
    private final int firstPortNumber = 42000;
    private final int lastPortNumber = 42500;

    // END OF CONFIG

    private Map<String, FFDEChannel> registeredServers = new ConcurrentHashMap<>(); //< registered servers by name
    private List<FFDEChannel> temporaryServersList = new CopyOnWriteArrayList<>();    //< stores servers prior to naming

    private String state = "registration";                              //< state of the FFDE network
    private final Object stateLock = new Object();                      //< synchronization lock

    private int time = 0;                                               //< counts time [ms]
    private final Object timeLock = new Object();                       //< synchronization lock

    private int lastUnusedPort = firstPortNumber;                       //< holds last port that is still unassigned
    private final Object lastUnusedPortLock = new Object();             //< synchronization lock

    private List<List<String>> eventLog = new CopyOnWriteArrayList<>(); //< stores important events from network
    private List<List<String>> errorLog = new CopyOnWriteArrayList<>(); //< stores errors from network

    private int timeToNextState = registrationDelay;                    //< stores time until next state
    private final Object timeToNextStateLock = new Object();            //< synchronization lock

    // FFDE utilities
    private FFDETimer timer = new FFDETimer(1);                                 //< timer for updating channels
    private FFDETimer clock = new FFDETimer(1, "timeTick");                     //< timer for measuring time...
    private FFDEEventDispatcher eventDispatcher = new FFDEEventDispatcher();    //< main event dispatcher of the server
    private ServersLinker serversLinker;                                        //< accepts servers during registration
    private SubscriptionsBank subscriptionsBank = new SubscriptionsBank();      //< stores registered sub channels

    private int kernelPort;                 //< kernel accepts connections with servers on this port

    public FFDEKernel(int aPort) {
        kernelPort = aPort;

        clock.observeFFDE(this, "timeTick");

        serversLinker = new ServersLinker(temporaryServersList, kernelPort, timer, eventDispatcher);

        timer.stopTimer();              //< do not let channels to generate events until their name is determined

        eventDispatcher.observeFFDE(this, "serv");

        // start timers
        Thread timerThread = new Thread(timer);
        timerThread.setName("timer");
        Thread clockThread = new Thread(clock);
        clockThread.setName("timer");
        timerThread.start();
        clockThread.start();

        // start dispatcher
        Thread dispatcherThread = new Thread(eventDispatcher);
        dispatcherThread.start();

        // start linker
        Thread linkerThread = new Thread(serversLinker);
        linkerThread.start();
    }


    /**
     * Overrides method from interface <code>FFDENetwork.FFDEObserver</code>.
     * @param aEvent    event from timer or dispatcher
     */
    @Override
    public void notifyFFDE(FFDEEvent aEvent) {
        String id = aEvent.getIdentifier();

        switch(id) {
            case "timeTick":
                int currentTime;
                synchronized (timeLock) {
                    time++;
                    currentTime = time;
                }

                synchronized (timeToNextStateLock) {
                    if(currentTime >= timeToNextState) {
                        time = 0;
                        clock.stopTimer();
                        setNextState();
                        clock.startTimer();
                    }
                }
                break;

            case "serv":
                handleMessageFromServer(aEvent.getMessage());

            default:
                break;
        }
    }

    /**
     * Returns a set of names of all registered servers. If no server is registered returns null. If the state is not
     * frozen returns null.
     * @return      set of names or null
     */
    public Set<String> getListOfRegisteredServers() {
        boolean operationPermitted = false;

        synchronized (stateLock) {
            if(state.equals("frozen"))
                operationPermitted = true;
        }

        if(operationPermitted) {
            return registeredServers.keySet();
        }
        else
            return null;
    }

    /**
     * Returns current state of the network.
     * @return      String - name of the state
     */
    public String getCurrentState() {
        synchronized (stateLock) {
            return state;
        }
    }

    /**
     * Deals with messages extracted from rx events coming from registered servers.
     * @param aMessage      a message extracted from event in from of a list
     */
    private void handleMessageFromServer(List<String> aMessage) {
        String command = aMessage.get(1);
        switch(command) {
            case "take_control":
                String context = aMessage.get(2);
                if(context.equals("1")) {
                    String slaveName = aMessage.get(3);
                    String masterName = aMessage.get(4);
                    int assignedPort = getNextFreePort();

                    sendToServer(slaveName, Arrays.asList("command", "take_control", "2", slaveName,
                            masterName, Integer.toString(assignedPort)));
                }
                else if(context.equals("3")) {
                    String slaveName = aMessage.get(3);
                    String masterName = aMessage.get(4);
                    String assignedPort = aMessage.get(5);

                    sendToServer(masterName, Arrays.asList("command", "take_control", "4", slaveName,
                            masterName, assignedPort));
                }
                else {
                    // TODO context error
                }

                break;

            case "open_pipeline":
                if(aMessage.get(2).equals("1")) {
                    String recipientName = aMessage.get(3);
                    String initiatorName = aMessage.get(4);
                    int assignedPort = getNextFreePort();

                    sendToServer(recipientName, Arrays.asList("command", "open_pipeline", "2", recipientName,
                            initiatorName, Integer.toString(assignedPort)));
                }
                else if(aMessage.get(2).equals("3")) {
                    String recipientName = aMessage.get(3);
                    String initiatorName = aMessage.get(4);
                    String assignedPort = aMessage.get(5);

                    sendToServer(initiatorName, Arrays.asList("command", "open_pipeline", "4", recipientName,
                            initiatorName, assignedPort));
                }
                else {
                    // TODO context error
                }

                break;

            case "publish":
                String sub = aMessage.get(3);
                String pub = aMessage.get(4);
                int prt = getNextFreePort();
                // register subscription
                subscriptionsBank.addSubscription(pub, sub, prt);
                // send command to publisher as a response to its request
                sendToServer(pub, Arrays.asList("command", "publish", "2", sub, pub, Integer.toString(prt)));
                break;

            case "subscribe":
                String pub2 = aMessage.get(3);
                String sub2 = aMessage.get(4);

                int prt2;
                if(subscriptionsBank.subscriptionExists(pub2, sub2)) {
                    prt2 = subscriptionsBank.getSubscriptionPort(pub2, sub2);
                }
                else {
                    // TODO send some error message
                    break;
                }

                sendToServer(aMessage.get(5), Arrays.asList("command", "subscribe", "2", pub2, sub2, aMessage.get(5),
                        Integer.toString(prt2)));
                break;

            case "crash":
                commitErrorMessage(aMessage);
                break;

            case "log":
                commitLogMessage(aMessage);
                break;

            default:        //< incorrect command
                break;

        }
    }

    /**
     * Commits a new log message to the kernel's event log.
     * @param aMessage      full message copied as-is from event
     */
    private void commitLogMessage(List<String> aMessage) {
        eventLog.add(aMessage);
    }

    /**
     * Commits a new error message to the kernel's log.
     * @param aMessage      full message copied as-is from event
     */
    private void commitErrorMessage(List<String> aMessage) {
        errorLog.add(aMessage);
    }

    /**
     * Transmit a message to the specified server.
     * @param aServer       server's name
     * @param aMessage      message
     */
    private void sendToServer(String aServer, List<String> aMessage) {
        registeredServers.get(aServer).transmitMessage(aMessage);
    }

    /**
     * Changes state of the whole FFDENetwork maintained by this kernel.
     * States are changed as follows:
     * <H1>registration -> declaration -> subscription -> frozen</H1>
     */
    private void setNextState() {
        String currentState;
        synchronized (stateLock) {
            currentState = state;
        }

        switch(currentState) {
            case "registration":
                // change state
                synchronized (stateLock) {
                    state = "declaration";
                }
                // set delay
                synchronized (timeToNextStateLock) {
                    timeToNextState = declarationDelay;
                }
                // stop accepting new servers for registration
                serversLinker.stopLinking();
                // move channels from temporary list to a target structure and set their names
                for (FFDEChannel channel : temporaryServersList) {
                    List<String> registrationMessage = channel.getRxBuffer();
                    // read name of the registered server from its registration message
                    if(registrationMessage.get(1).equals("register")) {
                        channel.setName(registrationMessage.get(3));
                    }
                    else {
                        // TODO deal with incorrect registration
                    }

                        /*
                        DANGER! - this thing should be synchronized but isn't as it would require nested synchronization
                        blocks and therefor could jam the application.
                        It is done in that way because it should be safe - initial new-state broadcast is the first
                        usage of "registeredServers" structure so now it is safe to access it without synchronization.
                         */
                    registeredServers.put(channel.getName(), channel);
                }

                // start timer responsible for updating channels
                timer.startTimer();
                // broadcast state update to all servers
                broadcast(Arrays.asList("command", "set_state", "1", "declaration"));
                break;

            case "declaration":
                // change state
                synchronized (stateLock) {
                    state = "subscription";
                }
                // set delay
                synchronized (timeToNextStateLock) {
                    timeToNextState = subscriptionDelay;
                }

                // nothing more needs to be done here

                broadcast(Arrays.asList("command", "set_state", "1", "subscription"));
                break;

            case "subscription":
                // change state
                synchronized (stateLock) {
                    state = "frozen";
                }

                clock.stopTimer();      // this timer is no longer needed

                broadcast(Arrays.asList("command", "set_state", "1", "frozen"));
                break;

            default:    //< wrong state
                break;
        }
    }

    /**
     * Determines and returns an unassigned port number from the range declared as safe.
     * @return      port number
     */
    private int getNextFreePort() {
        synchronized (lastUnusedPortLock) {
            int a = lastUnusedPort;
            if(lastUnusedPort < lastPortNumber) {
                lastUnusedPort++;
            }
            else {
                // TODO port out of safe range
            }

            return a;
        }
    }

    /**
     * Transmits a message to all servers registered in the kernel.
     * @param aMessage      message to be transmitted
     */
    private void broadcast(List<String> aMessage) {
        //Set<String> keysCopy = new TreeSet<>(registeredServers.keySet());
        // iterate through all registered servers and broadcast the message

        for(FFDEChannel ch : registeredServers.values())
            ch.transmitMessage(aMessage);

        /*for(String channel : keysCopy) {
            registeredServers.get(channel).transmitMessage(aMessage);
        }*/
    }

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //                                  UTILITY CLASSES                                           +
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    /**
     * Container class used to store subscription channels.
     */
    private class SubscriptionsBank {
        private Map<String, Hashtable<String, Integer>> subscriptions = new ConcurrentHashMap<>();

        public void addSubscription(String aPublisher, String aChannel, int aPort) {
            if(subscriptions.containsKey(aPublisher)) {
                subscriptions.get(aPublisher).put(aChannel, aPort);
            }
            else {
                Hashtable<String, Integer> newChannel = new Hashtable<>();
                subscriptions.put(aPublisher, newChannel);
                subscriptions.get(aPublisher).put(aChannel, aPort);
            }
        }

        /**
         * Checks if the specified subscription is registered in the kernel.
         * @param aPublisher    publisher of the subscription channel
         * @param aChannel      name of the channel
         * @return              <code>true</code> if it exists, otherwise <code>false</code>
         */
        public boolean subscriptionExists(String aPublisher, String aChannel) {
            if(subscriptions.containsKey(aPublisher)) {
                if(subscriptions.get(aPublisher).containsKey(aChannel))
                    return true;
            }
            return false;
        }

        /**
         * Checks where the specified subscription channel is available.
         * @param aPublisher    publisher of the subscription channel
         * @param aChannel      name of the channel
         * @return              int - number of the locahost port assigned for subscription
         */
        public int getSubscriptionPort(String aPublisher, String aChannel) {
            return subscriptions.get(aPublisher).get(aChannel);
        }
    }


    /**
     * Waits on specified port and accepts connections from all incoming FFDEServers.
     */
    private class ServersLinker implements Runnable {

        private volatile boolean stopFlag = false;                               //< stops the linker if true

        private ServerSocket serverSocket;              // server socket of the linker

        private List<FFDEChannel> servers;              // reference to temporary list of registered servers

        private FFDETimer timer;
        private FFDEEventDispatcher dispatcher;

        public ServersLinker(List<FFDEChannel> aRegisteredServers, int aPort, FFDETimer aTimer,
                             FFDEEventDispatcher aDispatcher) {

            servers = aRegisteredServers;
            timer = aTimer;
            dispatcher = aDispatcher;

            try {
                serverSocket = new ServerSocket(aPort);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            while(!checkFlag()) {
                try {
                    Socket link = serverSocket.accept();        //< the thread will halt here until smth connects
                    servers.add(new FFDEChannel(link, dispatcher, timer, "serv"));
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * Stops the linker. No more servers will be accepted.
         */
        public void stopLinking() {
            stopFlag = true;
        }

        /**
         * Safely returns stopFlag.
         * @return      boolean stopFlag
         */
        private boolean checkFlag() {
                return stopFlag;
        }
    }
}
