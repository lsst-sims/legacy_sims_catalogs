import java.util.*;

public class DisplayLists {
    public DisplayLists() {
        InputObsIds = new ArrayList<String>();
        UnQueuedObsIds = new ArrayList<String>();
        QueuedObsIds = new ArrayList<String>();
        RunningObsIds = new ArrayList<String>();
        RetriedObsIds = new ArrayList<String>();
        FinishedObsIds = new ArrayList<String>();
        FailedObsIds = new ArrayList<String>();
        JobRunTimes = new ArrayList<Integer>();
        
        AllLists = new ArrayList< List<String> >();
        AllLists.add(InputObsIds);
        AllLists.add(UnQueuedObsIds);
        AllLists.add(QueuedObsIds);
        AllLists.add(RunningObsIds);
        AllLists.add(RetriedObsIds);
        AllLists.add(FinishedObsIds);
        AllLists.add(FailedObsIds);
    }

    public List<String> InputObsIds;
    public List<String> UnQueuedObsIds;
    public List<String> QueuedObsIds;
    public List<String> RunningObsIds;
    public List<String> RetriedObsIds;
    public List<String> FinishedObsIds;
    public List<String> FailedObsIds;
    public List<Integer> JobRunTimes;

    public List< List<String> > AllLists;
}
