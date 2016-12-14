package FFDENetwork;

import java.util.*;
import java.net.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by JM on 25.03.2016.
 * This class is a container for a set of channels to modules that subscribe a certain set
 * of data. (ex. filtering module would probably want to subscribe IMU readings)
 * Every update is automatically transmitted to all subscribers in a separate thread.
 */
public class FFDEPublisher implements Runnable {

    private List<FFDEChannel> channels = new CopyOnWriteArrayList<>();    //< list of channels in subscription

    private int port;                                           //< port of the publisher
    private FFDETimer timer;                                    //< reference to a timer used to update channels

    private String name = "";       //< an identifier of a subscription (ex. "Acc" for accelerometer)

    /**
     * constructor
     */
    public FFDEPublisher(String aName, int aPort, FFDETimer aTimerForChannels) {
        name = aName;
        port = aPort;
        timer = aTimerForChannels;
    }

    /**
     * overrides 'run()' from Runnable interface - enables using as a separate thread
     */
    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        while(true) {
            Socket newSubscriber = null;
            try {
                newSubscriber = serverSocket.accept();      //< the thread will halt here and wait for clients
                FFDEChannel newChannel = new FFDEChannel(name + "_subscriber", newSubscriber, null,
                        timer, "ERROR");
                channels.add(newChannel);
            }
            catch(Exception e) {
                // TODO The program will crash but I don't want to solve this right now.
                e.printStackTrace();
            }
            // add a new subscriber
            /* TODO
                channels used to publish subscriptions can not generate rx events but this ability is still unlocked so
                that kind of activity can crash whole application

            FFDEChannel newChannel = new FFDEChannel(name + "_subscriber", newSubscriber, null,
                    timer, "ERROR");
            System.out.println("things");
            channels.add(newChannel);*/
        }
    }

    /**
     * Injects new data into subscription and gets it immediately transmitted to all
     * subscribers.
     * @param aNewData  fresh package of data for subscribers grouped into lines of text
     */
    public void update(List<String> aNewData) {
        //synchronized (channelsLock) {
               if(!channels.isEmpty()) {
                   for(FFDEChannel channel : channels) {
                       channel.transmitMessage(aNewData);
                   }
               }
        //}
    }

    /**
     * Gets a name of the subscription publisher.
     * @return      name of the subscription
     */
    public String getName() {
        return name;
    }

    /**
     * Removes a channel from subscription. (available overload with String param)
     * @param channel           reference to the channel that requires closing
     */
    @Deprecated
    public void unsubscribe(FFDEChannel channel) {
        channels.remove(channel);
    }
}
