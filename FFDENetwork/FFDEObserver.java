package FFDENetwork;

/**
 * Created by JM on 31.03.2016.
 * Event observer interface similar to Java native Observer but associated with FFDENetwork.FFDEEventDispatcher rather than
 * Observable object. Dispatcher can deal witch any number of types of FFDEEvents witch allows to observe only the
 * server's global dispatcher rather than individual sources.
 *
 *  */
public interface FFDEObserver {

    void notifyFFDE(FFDEEvent event);
}
