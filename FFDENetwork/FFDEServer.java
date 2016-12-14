package FFDENetwork;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Started by JM on 25.03.2016.
 * Finished by JM on 10.05.2016.
 * TODO
 */
public class FFDEServer implements FFDEObserver{

    private Map<String, FFDEPublisher> subscriptionOutputs = new ConcurrentHashMap<>();   //< map of published channels
    private Map<String, FFDEChannel> slavesChannels = new ConcurrentHashMap<>();  //< channels to slave servers
    private List<FFDEChannel> subscriptionInputs = new CopyOnWriteArrayList<>();  //< data from subscribed channels in
    private Map<String, FFDEChannel> pipelineChannels = new ConcurrentHashMap<>();        //< stores pipelines

    private volatile FFDEChannel commandChannel;                //< channel to the commander of the server
    private volatile FFDEChannel kernelChannel;                 //< channel to the kernel server on 666 port

    private String serverState = "registration";                            //< state of the FFDE network
    private final Object serverStateLock = new Object();                    //< synchronization lock

    // FFDE utilities
    private FFDETimer timer = new FFDETimer(1);                                 //< timer for updating channels
    private FFDEEventDispatcher internalEventDispatcher = new FFDEEventDispatcher();    //< main event dispatcher
    private FFDEEventDispatcher externalEventDispatcher = new FFDEEventDispatcher(1);   //< dispatcher for the client
    private InitializationRequestsQueue kernelRequestsQueue;                    //< queue for kernel requests

    private String nodeName;                    //< name of the server's node
    private int kernelsPort;                    //< port of the kernel
    private FFDEObserver node;                  //< node that created an instance of this server

    /**
     * Creates a complete instance of an FFDENetwork.FFDEServer. Automatically initializes and reports the
     * server to the kernel.
     * @param aName             name of the node
     * @param aKernelsPort      port of the kernel
     */
    public FFDEServer(String aName, int aKernelsPort, FFDEObserver aNode) {
        // set name
        nodeName = aName;

        // set reference to the master node
        node = aNode;

        // set kernels port
        kernelsPort = aKernelsPort;

        // setup kernel requests queue
        kernelRequestsQueue = new InitializationRequestsQueue(Arrays.asList("declaration",
                "subscription", "frozen"));     //< no request can possibly be linked to "registration" state

        // start observing dispatcher
        internalEventDispatcher.observeFFDE(this, "krx");       //< message received from kernel
        internalEventDispatcher.observeFFDE(this, "ktx");       //< message for kernel

        // initialize kernel channel
        try {
            Socket socket = new Socket("localhost", kernelsPort);
            kernelChannel = new FFDEChannel("kernel", socket, internalEventDispatcher, timer, "krx");
        } catch (IOException e) {         //< if this exception is caught then server can not start
            e.printStackTrace();
        }

        // report to the kernel
        List<String> message = Arrays.asList("request", "register", "1", nodeName);
        kernelChannel.transmitMessage(message);

        // start timer
        Thread timerThread = new Thread(timer);
        timerThread.setName("timer");
        timerThread.start();

        // start internal dispatcher
        Thread internalDispatcherThread = new Thread(internalEventDispatcher);
        internalDispatcherThread.setName("internalDispatcher");
        internalDispatcherThread.start();

        // start external dispatcher
        Thread externalDispatcherThread = new Thread(externalEventDispatcher);
        externalDispatcherThread.setName("externalDispatcher");
        externalDispatcherThread.start();
    }

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //                                     !!! SERVICES !!!                                     +
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    /**
     * Adds a new publishing stream to the server.
     * @param aName     name of the data channel that is being published, ex. 'acc' , 'gyro' etc.
     */
    public void publish(String aName) {
        // create a request in form of an ktx event and enqueue it until a proper net state is reached
        List<String> requestToKernel = Arrays.asList("request", "publish", "1", aName, nodeName);
        FFDEEvent ktxEvent = new FFDEEvent("ktx", requestToKernel);
        kernelRequestsQueue.addRequest(ktxEvent, "declaration");
    }

    /**
     * Takes control over the specified server and adds it to connections as a slave.
     * @param aName     name of the slave server
     */
    public void takeControl(String aName) {
        // create a request in form of an ktx event and enqueue it until a proper net state is reached
        List<String> request = Arrays.asList("request", "take_control", "1", aName, nodeName);
        FFDEEvent ktxEvent = new FFDEEvent("ktx", request);
        kernelRequestsQueue.addRequest(ktxEvent, "declaration");
    }

    /**
     * Subscribes specified data channel registered in FFDE network.
     * @param aPublisher    name of the publishing node, ex. 'kalman'
     * @param aChannel      name of the data channel, ex. 'acc'
     */
    public void subscribe(String aPublisher, String aChannel) {
        // create a request in form of ktx event and enqueue it until a proper network state is reached
        List<String> request = Arrays.asList("request", "subscribe", "1", aPublisher, aChannel, nodeName);
        FFDEEvent ktxEvent = new FFDEEvent("ktx", request);
        kernelRequestsQueue.addRequest(ktxEvent, "subscription");
    }

    /**
     * Opens pipeline connected to a specified node registered in the same FFDE Network.
     * @param aName     name of the pipeline recipient
     */
    public void openPipeline(String aName) {
        // create a request in form of an ktx event and enqueue it until a proper net state is reached
        List<String> request = Arrays.asList("request", "open_pipeline", "1", aName, nodeName);
        FFDEEvent ktxEvent = new FFDEEvent("ktx", request);
        kernelRequestsQueue.addRequest(ktxEvent, "declaration");
    }

    /**
     * Sends data to the specified slave.
     * @param aSlave        name of the slave
     * @param aData         data in form of a list of Strings
     */
    public void sendToSlave(String aSlave, List<String> aData) {
        if(slavesChannels.containsKey(aSlave)) {
            slavesChannels.get(aSlave).transmitMessage(aData);
        }
        else {
            // TODO if required slave does not exist do something
        }
    }

    /**
     * Sends data to the specified pipeline.
     * @param aRecipient    name of the recipient
     * @param aData         data in form of a list of Strings
     */
    public void sendThroughPipeline(String aRecipient, List<String> aData) {
        if(pipelineChannels.containsKey(aRecipient)) {
            pipelineChannels.get(aRecipient).transmitMessage(aData);
        }
        else {
            // TODO if required pipeline does not exist do something
        }
    }

    /**
     * Sends data to the master.
     * @param aData     data in form of a list of Strings
     */
    public void sendToMaster(List<String> aData) {
        commandChannel.transmitMessage(aData);
    }

    /**
     * Sends data to all servers subscribing specified data channel.
     * @param aChannel      name of the channel
     * @param aMessage      data to be transmitted
     */
    public void updateDataSubChannel(String aChannel, List<String> aMessage) {
        if(subscriptionOutputs.containsKey(aChannel)) {
            subscriptionOutputs.get(aChannel).update(aMessage);
        }
    }

    /**
     * The calling thread will halt here until the network is ready to handle data transmission.
     */
    public void waitUntilNetworkIsReady() {
        // wait for correct server state
        while(!getState().equals("frozen")) {
            try{
                Thread.sleep(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // wait for an additional few milliseconds
        try {
            Thread.sleep(5);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets all active pipelines.
     * @return      set of names of connected nodes
     */
    public Set<String> getActivePipelines() {
        return pipelineChannels.keySet();
    }

    //                                      END OF SERVICES                                     +
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    /**
     * Overrides <code>notifyFFDE()</code> from <code>FFDENetwork.FFDEObserver interface</code>. The server observes
     * its dispatcher for events associated with kernel.
     * @param aEvent     event that triggered this method
     */
    @Override
    public void notifyFFDE(FFDEEvent aEvent) {
        String id = aEvent.getIdentifier();

        switch (id) {
            case "ktx":
                handle_ktx_event(aEvent);
                break;

            case "krx":
                handle_krx_event(aEvent);
                break;

            default:
                break;
        }
    }

    /**
     * Gets a name of the node.
     * @return          name as String
     */
    public String getName() {
        return nodeName;
    }

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //                       METHODS FOR HANDLING INCOMING KERNEL COMMANDS                      +
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    /**
     * Events with 'ktx' identifier are intended to be retransmitted to kernel and do not trigger any other actions.
     * @param aEvent    ktx event to be retransmitted
     */
    private void handle_ktx_event(FFDEEvent aEvent) {
        kernelChannel.transmitMessage(aEvent.getMessage());
    }

    /**
     * Handles any krx event. This method is designed to be called by an external thread from the event dispatcher.
     * @param aEvent        FFDENetwork.FFDEEvent with 'krx' identifier (coming from kernel)
     */
    private void handle_krx_event(FFDEEvent aEvent) {
        String type = aEvent.getMessage().get(1);

        switch(type) {
            case "take_control":
                handle_take_control(aEvent.getMessage());
                break;

            case "publish":
                handle_publish(aEvent.getMessage());
                break;

            case "subscribe":
                handle_subscribe(aEvent.getMessage());
                break;

            case "set_state":
                handle_set_state(aEvent.getMessage());
                break;

            case "open_pipeline":
                handle_open_pipeline(aEvent.getMessage());
                break;

            default:
                // TODO unknown command type handler
                break;
        }
    }

    /**
     * Handles command "open_pipeline" from the kernel.
     * @param aMessage  message from an rx_event with the command
     */
    private void handle_open_pipeline(List<String> aMessage) {
        // note that this command needs to be handled in two different contexts on server's side

        String context = aMessage.get(2);   //< extract command context

        switch (context) {
            case "2":   // the server is a recipient
                // prepare a socket to accept initiator connection
                new Thread(() -> {
                    try {
                        ServerSocket serverSocket = new ServerSocket(Integer.parseInt(aMessage.get(5)));
                        Socket socket = serverSocket.accept();

                        pipelineChannels.put(aMessage.get(4), new FFDEChannel(aMessage.get(4), socket,
                                externalEventDispatcher, timer, "pipe_" + aMessage.get(4)));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();

                // send response to the kernel
                List<String> response = Arrays.asList("response", "open_pipeline", "3", aMessage.get(3),
                        aMessage.get(4), aMessage.get(5));
                kernelChannel.transmitMessage(response);      //< transmit message

                externalEventDispatcher.observeFFDE(node, "pipe_" + aMessage.get(4));//< node: start observing rx event
                break;

            case "4":   // the server is an initiator
                try {
                    Socket socket = new Socket("localhost", Integer.parseInt(aMessage.get(5)));
                    String identifier = "pipe_" + aMessage.get(3);
                    FFDEChannel channel = new FFDEChannel(aMessage.get(3), socket, externalEventDispatcher, timer,
                            identifier);

                    // add the new channel to the map of slaves indexed by its name
                    pipelineChannels.put(aMessage.get(3), channel);

                    externalEventDispatcher.observeFFDE(node, "pipe_" + aMessage.get(3)); //< observe rx event
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;

            default:
                // TODO context error
                break;
        }
    }

    /**
     * Handles command "take_control" from the kernel.
     * @param aMessage  message from an rx_event with the command
     */
    private void handle_take_control(List<String> aMessage) {
        // note that this command needs to be handled in two different contexts on server's side

        String context = aMessage.get(2);   //< extract command context

        switch (context) {
            case "2":   // the server is a slave
                // prepare a socket to accept master connection
                new Thread(() -> {
                    try {
                        ServerSocket serverSocket = new ServerSocket(Integer.parseInt(aMessage.get(5)));
                        Socket socket = serverSocket.accept();

                        commandChannel = new FFDEChannel("commander", socket, externalEventDispatcher, timer, "crx");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();

                // send response to the kernel
                List<String> response = Arrays.asList("response", "take_control", "3", aMessage.get(3), aMessage.get(4),
                        aMessage.get(5));
                kernelChannel.transmitMessage(response);      //< transmit message

                externalEventDispatcher.observeFFDE(node, "crx");     //< the node starts observing rx event from master
                break;

            case "4":   // the server is a master
                try {
                    Socket socket = new Socket("localhost", Integer.parseInt(aMessage.get(5)));
                    String identifier = "srx_" + aMessage.get(3);
                    FFDEChannel channel = new FFDEChannel(aMessage.get(3), socket, externalEventDispatcher, timer,
                            identifier);

                    // add the new channel to the map of slaves indexed by its name
                    slavesChannels.put(aMessage.get(3), channel);

                    externalEventDispatcher.observeFFDE(node, "srx_" + aMessage.get(3)); //< observe rx event from slave
                } catch(IOException e) {
                    e.printStackTrace();
                }
                break;

            default:
                // TODO context error
                break;
        }
    }

    /**
     * Handles command "publish" from the kernel.
     * @param aMessage  message from an rx_event with the command
     */
    private void handle_publish(List<String> aMessage) {
        FFDEPublisher newPublishedSub = new FFDEPublisher(aMessage.get(3), Integer.parseInt(aMessage.get(5)), timer);

        Thread newPublisherThread = new Thread(newPublishedSub);
        newPublisherThread.start();

        subscriptionOutputs.put(newPublishedSub.getName(), newPublishedSub);
    }

    /**
     * Handles command "subscribe" from the kernel - connects this server to publisher of other server.
     * @param aMessage  message from an rx_event with the command
     */
    private void handle_subscribe(List<String> aMessage) {

        try {
            Socket socket = new Socket("localhost", Integer.parseInt(aMessage.get(6)));
            String sub_name = "sub_" + aMessage.get(3) + "." + aMessage.get(4); //< publisher.data_channel
            FFDEChannel channel = new FFDEChannel(sub_name, socket, externalEventDispatcher, timer, sub_name);

            subscriptionInputs.add(channel);

            externalEventDispatcher.observeFFDE(node, sub_name);    //< event: sub_<publisher>.<channel>
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handles command "take_control" from the kernel.
     * @param aMessage  message from an rx_event with the command
     */
    private void handle_set_state(List<String> aMessage) {
        setState(aMessage.get(3));
    }


    //                                                       END OF KERNEL HANDLERS             +
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    /**
     * Safely sets state of the server and deploys kernel requests associated with the new state. Available states:
     * <ul>
     *     <li>registration</li>
     *     <li>declaration</li>
     *     <li>subscription</li>
     *     <li>frozen</li>
     * </ul>
     * @param aState    new state as String
     */
    private void setState(String aState) {
        synchronized (serverStateLock) {
            switch (aState) {

                case "registration":
                    serverState = "registration";
                    break;

                case "declaration":
                    serverState = "declaration";
                    kernelRequestsQueue.deploy(serverState, internalEventDispatcher);       //< release awaiting events
                    break;

                case "subscription":
                    serverState = "subscription";
                    kernelRequestsQueue.deploy(serverState, internalEventDispatcher);       //< release awaiting events
                    break;

                case "frozen":
                    serverState = "frozen";
                    kernelRequestsQueue.deploy(serverState, internalEventDispatcher);       //< release awaiting events
                    break;

                default:
                    // TODO reaction to an unspecified state
                    break;
            }
        }
    }

    /**
     * Safely returns state of the server.
     * @return state of the server as a String
     */
    public String getState() {
        synchronized (serverStateLock) {
            return serverState;
        }
    }

    /**
     * This utility class is used to hold requests for kernel until FFDE network reaches state required for them to
     * operate correctly. For example: command "subscribe" will wait until channel feeding subscribed data is
     * available.
     */
    private final class InitializationRequestsQueue {

        private Map<String, List<FFDEEvent>> requests = new ConcurrentHashMap<>();

        /**
         * Creates a new instance of a multi-queue storage initialized with specified list of states.
         * @param aStates   list of states supported by this instance
         */
        public InitializationRequestsQueue(List<String> aStates) {
            for(String state : aStates) {
                requests.put(state, new LinkedList<>());
            }
        }

        /**
         * Adds a request to the queue related to the specified event.
         * @param aEvent            request
         * @param aState            state that request relates to
         */
        public void addRequest(FFDEEvent aEvent, String aState) {
            if(requests.containsKey(aState)) {
                requests.get(aState).add(aEvent);
            }
        }

        /**
         * Move all events related to specified state to a dispatcher.
         * @param aState            events related to this state will be moved
         * @param dispatcher        events are moved to this dispatcher
         */
        public void deploy(String aState, FFDEEventDispatcher dispatcher) {
            // this delay prevents errors caused by some mysterious problem with resource availability - seems to work
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if(requests.containsKey(aState)) {
                List<FFDEEvent> events = requests.get(aState);

                for(FFDEEvent event : events) {
                    dispatcher.reportEvent(event);
                }

                requests.remove(aState);    //< remove the state from queues
            }
        }
    }
}
