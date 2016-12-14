package FFDENetwork;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by JM on 26.03.2016.
 * This object is meant to generate an event every given amount of time. Then it uses interface similar to
 * FFDEDispatcher to notify all its observers with a 'tick' event. Updating all observers is done with the use of
 * multiple threads.
 *
 * <H1>Safety note:  <I>If updating all observers takes more than 1[ms] then the timer will jam. In that case it
 *                  is recommended to increase the number of worker threads as it would speed up the update process.</I>
 * </H1>
 */
public class FFDETimer implements Runnable, FFDEObservable {
    private int tickInterval = 1;                                   //< tick interval [ms]
    //private ExecutorService executorAssignService = Executors.newSingleThreadExecutor();    //< assign executor
    //private ExecutorService executorUpdateService = Executors.newFixedThreadPool(4);        //< update executors

    private List<FFDEObserver> observers = new CopyOnWriteArrayList<>();    //< list of observers of the 'tick' event
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);   //< executes ticks

    private boolean onFlag = true;                                  //< when this flag is set the timer generates ticks
    private final Object onFlagLock = new Object();                 //< synchronization lock

    String eventName;
    private FFDEEvent tickEvent;

    /**
     * Creates a new instance of FFDENetwork.FFDETimer. Generated event is called "tick".
     * @param aTickInterval     interval in [ms]
     */
    public FFDETimer(int aTickInterval) {
        tickInterval = aTickInterval;
        eventName = "tick";
        tickEvent = new FFDEEvent(eventName, null);
    }

    /**
     * Creates a new instance of FFDENetwork.FFDETimer. Generated event name is specified in the argument.
     * @param aTickInterval     interval in [ms]
     * @param aEventName        name of the event
     */
    public  FFDETimer(int aTickInterval, String aEventName) {
        tickInterval = aTickInterval;
        eventName = aEventName;
        tickEvent = new FFDEEvent(eventName, null);
    }

    /**
     * Overrides run() from Runnable. Generates tick every 1[ms]
     */
    @Override
    public void run() {
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if(checkOnFlag()) {                     //< generate ticks only when onFlag is set
                    for(FFDEObserver o : observers) {
                        o.notifyFFDE(tickEvent);
                    }
                }
            }
        }, 0, 1, TimeUnit.MILLISECONDS);


//        while(true) {
//            try {
//                Thread.sleep(tickInterval);         //< make interval
//            } catch (Exception e) {      //< if this happens the timer is lost anyway, no recovery provided
//                e.printStackTrace();
//            }
//            if(checkOnFlag()) {                     //< generate ticks only when onFlag is set
//                for(FFDEObserver o : observers) {
//                    o.notifyFFDE(tickEvent);
//                }
//            }
//        }
    }

    /**
     * Start observing an event.
     * @param aObserver         reference to the observer
     * @param aEvent            event ID string
     */
    @Override
    public void observeFFDE(FFDEObserver aObserver, String aEvent) {
        if(aEvent.equals(eventName)) {
            observers.add(aObserver);
        }
    }

    /**
     * Start observing an event. The only observable event from the timer is 'tick'
     * @param aObserver         reference to the observer
     * @param aEvent            event ID string (the only viable is 'tick')
     * @param aParam            does not matter
     */
    @Override
    public void observeFFDE(FFDEObserver aObserver, String aEvent, boolean aParam) {
        observeFFDE(aObserver, aEvent);
    }

    /**
     * Stop observing an event. The only observable event from the timer is 'tick'
     * @param aObserver         reference to the observer
     * @param aEvent            event ID string (the only viable is 'tick')
     */
    @Override
    public void stopObservationFFDE(FFDEObserver aObserver, String aEvent) {
        if(aEvent.equals(eventName))
            observers.remove(aObserver);
    }

    /**
     * Stop observing all events. (timer has only one observable event so it does not really matter)
     * @param aObserver         reference to the observer
     */
    @Override
    public void stopAllObservationsFFDE(FFDEObserver aObserver) {
        stopObservationFFDE(aObserver, eventName);
    }

    /**
     * Safely checks state of the <code>onFlag</code>
     * @return      state of the flag
     */
    private boolean checkOnFlag() {
        synchronized (onFlagLock) {
            return onFlag;
        }
    }

    /**
     * Timer resumes. If it is already running nothing happens.
     */
    public void startTimer() {
        synchronized (onFlagLock) {
            onFlag = true;
        }
    }

    /**
     * Timer stops. If it is already stopped nothing happens.
     */
    public void stopTimer() {
        synchronized (onFlagLock) {
            onFlag = false;
        }
    }
}
