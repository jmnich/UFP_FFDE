package FFDENetwork;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by JM on 27.03.2016.
 * The heart of event distribution in FFDENetwork.FFDEServer.
 * Usage:
 * <ul>
 *     <li>events get registered when they get their first observer</li>
 *     <li>events which are not observed get to the archive</li>
 *     <li>when an event is reported all its observers are immediately notified</li>
 *     <li>observers are updated by multiple threads simultaneously</li>
 *     <li>one observer can observe more than one event coming from dispatcher</li>
 * </ul>
 */
public class FFDEEventDispatcher implements Runnable, FFDEObservable {

    // this field stores lists of observers for all event types
    private Map<String, List<FFDEObserver>> eventsObservers = new ConcurrentHashMap<>();

    private Queue<FFDEEvent> eventQueue = new ConcurrentLinkedQueue<>();   //< common queue for all FFDEEvents
    private Queue<FFDEEvent> archiveQueue = new ConcurrentLinkedQueue<>(); //< queue for events reported when unobserved

    // thread used to assign tasks for other executor services (this is different than in timer due to safety reasons)
    private ExecutorService executorAssignService = Executors.newSingleThreadExecutor();
    private ExecutorService executorUpdateService;    //< regular worker threads
    private ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    /**
     * Creates a dispatcher with no registered events.
     */
    public FFDEEventDispatcher() {
        executorUpdateService = Executors.newFixedThreadPool(4);
    }

    /**
     * Creates a dispatcher that uses a specified number of worker threads to update its oservers.
     * @param aNumberOfThreads      desired number of threads
     */
    public  FFDEEventDispatcher(int aNumberOfThreads) {
        if(aNumberOfThreads == 1)
            executorUpdateService = Executors.newSingleThreadExecutor();
        else
            executorUpdateService = Executors.newFixedThreadPool(aNumberOfThreads);
    }

    /**
     * Creates a dispatcher with one event registered.
     * @param aEvent    an identifier of FFDENetwork.FFDEEvent to be registered
     */
    @Deprecated         //< now events don't require explicit registration
    public FFDEEventDispatcher(String aEvent) {
        executorUpdateService = Executors.newFixedThreadPool(4);
        registerEvent(aEvent);
    }

    /**
     * Creates a dispatcher with a list of events registered.
     * @param aEvents   a list of identifiers of FFDEEvents to be registered
     */
    @Deprecated         //< now events don't require explicit registration
    public FFDEEventDispatcher(List<String> aEvents) {
        executorUpdateService = Executors.newFixedThreadPool(4);
        for(String evt : aEvents)
            registerEvent(evt);
    }

    /**
     * overrides run() from Runnable. Inside this method the Dispatcher watches its event queue and when an event
     * appears it immediately attempts to assign one of its worker events to update all observers associated with
     * the event.
     */
    @Override
    public void run() {
        scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                while(eventInQueue()) {
                    FFDEEvent processedEvent;
                    processedEvent = eventQueue.remove();

                    // if the event is registered handle it
                    if (eventsObservers.containsKey(processedEvent.getIdentifier())) {
                        List<FFDEObserver> listOfObservers = eventsObservers.get(processedEvent.getIdentifier());

                        for(FFDEObserver o : listOfObservers) {
                            executorUpdateService.execute(() -> o.notifyFFDE(processedEvent));
                        }

                        // now send a new task dispatcher object to the queue of assigning thread (blah blah blah)
//                    executorAssignService.execute(new ObserverUpdateAssigner(listOfObservers, executorUpdateService,
//                            processedEvent));

                    } else {
                        // TODO this block of code should put something into error log
                    } //< now the 'eventObserversLock' is released and the executor begins its job
                }
            }
        }, 0, 1, TimeUnit.MILLISECONDS);

//        while(true) {
//            // if the eventQueue contains a new event
//            if(eventInQueue()) {
//                FFDEEvent processedEvent;
//                processedEvent = eventQueue.remove();
//
//                // if the event is registered handle it
//                if (eventsObservers.containsKey(processedEvent.getIdentifier())) {
//                    List<FFDEObserver> listOfObservers = eventsObservers.get(processedEvent.getIdentifier());
//
//                    for(FFDEObserver o : listOfObservers) {
//                        executorUpdateService.execute(() -> o.notifyFFDE(processedEvent));
//                    }
//
//                    // now send a new task dispatcher object to the queue of assigning thread (blah blah blah)
////                    executorAssignService.execute(new ObserverUpdateAssigner(listOfObservers, executorUpdateService,
////                            processedEvent));
//
//                } else {
//                    // TODO this block of code should put something into error log
//                } //< now the 'eventObserversLock' is released and the executor begins its job
//            }
//            else{
//                try {
//                    Thread.sleep(1);                //< allows other threads to access event queue
//                }
//                catch(InterruptedException e) {}    //< probably no need to react on this
//            }
//        }
    }

    /**
     * Start observing the specified event and remove archival events of that type.
     * @param aObserver         reference to FFDENetwork.FFDEObserver to be associated with the event
     * @param aEvent            identifier String of the event to be observed
     */
    @Override
    public void observeFFDE(FFDEObserver aObserver, String aEvent) {
        observeFFDE(aObserver, aEvent, false);          //< call overload and remove archival events
    }

    /**
     * Start observing the specified event and decide what to do with archival events of thar type.
     * @param aObserver         reference to FFng of the event to be observed
     * @param aParam            what to do witDEObserver to be associated with the event
     * @param aEvent            identifier Strih archival events: true - send to queue, ,false - remove from archive
     */
    @Override
    public void observeFFDE(FFDEObserver aObserver, String aEvent, boolean aParam) {
        //boolean eventRegistered = true;
        if(!eventsObservers.containsKey(aEvent)) {      //< if the event is unregistered then register it
            eventsObservers.put(aEvent, new LinkedList<FFDEObserver>());
            //eventRegistered = false;
        }
        eventsObservers.get(aEvent).add(aObserver);     //< add the observer

        // now deal with archival events
        int initialSize = archiveQueue.size();
        for(int i = 0; i < initialSize - 1; i++) {
            FFDEEvent dummy = archiveQueue.remove();            //< remove every event from the archive queue
            if(dummy.getIdentifier().equals(aEvent)) {
                if(aParam)                                      //< if param = true push event to the main queue
                    eventQueue.add(dummy);
            }
            else                                                //< if event is of a different type push it back
                archiveQueue.add(dummy);
        }
    }

    /**
     * Stop observing the specified event.
     * @param aObserver         reference to a current FFDENetwork.FFDEObserver associated with the specified event
     * @param aEvent            identifier String of the event to be unobserved
     */
    @Override
    public void stopObservationFFDE(FFDEObserver aObserver, String aEvent) {
        if(eventsObservers.containsKey(aEvent)) {
            if(eventsObservers.get(aEvent).contains(aObserver)) {
                eventsObservers.get(aEvent).remove(aObserver);
            }
        }
    }

    /**
     * Stop observing all events associated with the observer.
     * @param aObserver         reference to a current FFDENetwork.FFDEObserver
     */
    @Override
    public void stopAllObservationsFFDE(FFDEObserver aObserver) {
        for(String key : eventsObservers.keySet()) {
            if(eventsObservers.get(key).contains(aObserver))
                eventsObservers.get(key).remove(aObserver);
        }
    }


    /**
     * Adds an event to the event queue. THIS IS NOT AN EVENT REGISTRATION!
     * @param aEvent            an FFDENetwork.FFDEEvent to be handled
     */
    public void reportEvent(FFDEEvent aEvent) {
        eventQueue.add(aEvent);
    }

    /**
     * Utility method telling whether the eventQueue contains an event. This method is thread-safe.
     * @return      true - queue contains an event; false - queue contains no event
     */
    private boolean eventInQueue() {
        return !eventQueue.isEmpty();
    }

    /**
     * Registers a new type of event in dispatcher. (note: changed from public to private 31.03.16 JM)
     * @param aEventIdentifier   identifier of an event
     */
    private void registerEvent(String aEventIdentifier) {
        eventsObservers.put(aEventIdentifier, new CopyOnWriteArrayList<>());
    }

    /**
     * This object notifies an observer with a specified FFDENetwork.FFDEEvent object in separate Thread. This ensures that one
     * observer performing time-consuming tasks can't delay notification of other observers.
     */
    private class EventHandler implements Runnable {
        private FFDEEvent event;
        private FFDEObserver observer;

        public EventHandler(FFDEEvent aEvent, FFDEObserver aObserver){
            event = aEvent;
            observer = aObserver;
        }

        public void run() {
            observer.notifyFFDE(event);     //< notify the observer
        }
    }

    /**
     * This object assigns worker threads from the pool passed in constructor arguments to observers through
     * EventHandler objects.
     */
    private class ObserverUpdateAssigner implements Runnable {
        private ExecutorService executors;          //< reference to externally initialized executor service
        private List<FFDEObserver> observers;       //< list of observers to be updated
        private FFDEEvent event;

        public ObserverUpdateAssigner(List<FFDEObserver> aObservers, ExecutorService aExecutors, FFDEEvent aEvent) {
            executors = aExecutors;
            observers = aObservers;
            event = aEvent;
        }

        /**
         * updates all observers, holds the synchronization lock associated with observers until executor service
         * finishes its job
         */
        public void run() {
            if (!observers.isEmpty()) {                             //< if the list contains any observer
                List<Future> controlObjects = new LinkedList<>();   //< list of 'Future' associated with tasks
                for (FFDEObserver o : observers) {
                    // assign worker-threads to update observers, append 'Future' objects to the list
                    controlObjects.add(executors.submit(new EventHandler(event, o)));
                }
                while(!allTasksDone(controlObjects));   //< wait until all task are done until releasing the lock
            }
        }

        /**
         * Checks if a set of tasks is completed.
         * @param aControlObjects   list of <code>Future</code> objects associated with this tasks
         * @return                  true if tasks are done, false if not
         */
        private boolean allTasksDone(List<Future> aControlObjects) {
            for(Future future : aControlObjects) {
                if(!future.isDone())
                    return false;
            }
            return true;
        }
    }
}
