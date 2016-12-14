package FFDENetwork;

/**
 * Created by JM on 01.04.2016.
 * This interface allows FFDEObservers to associate with a certain object generating FFDEEvents.
 */
public interface FFDEObservable {
    void observeFFDE(FFDEObserver aObserver, String aEvent);                    //< associates observer with observable
    void observeFFDE(FFDEObserver aObserver, String aEvent, boolean aParam);    //< as above + gives extra parameter
    void stopObservationFFDE(FFDEObserver aObserver, String aEvent);            //< stops observing one type of event
    void stopAllObservationsFFDE(FFDEObserver aObserver);                       //< stops observing all events
}
