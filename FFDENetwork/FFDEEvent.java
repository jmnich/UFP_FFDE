package FFDENetwork;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by JM on 27.03.2016.
 * Event used inside FFDE network together with FFDENetwork.FFDEEventDispatcher
 */
public class FFDEEvent {
    private String identifier = "";                     //< identifier of the event, assigns it to its observers
    private List<String> message = new LinkedList<>();  //< data carried by the event

    /**
     * An instance of the event.
     * @param aIdentifier   allows the event to be assigned to its observers
     * @param aMessage      data carried by the event
     */
    public FFDEEvent(String aIdentifier, List<String> aMessage) {
        identifier = aIdentifier;
        message = aMessage;
    }

    /**
     * Returns identifier of the event.
     * @return              String - identifier of an event
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Returns data carried by the event.
     * @return              List of Strings
     */
    public List<String> getMessage() {
        return message;
    }

}
