import java.io.*;
import java.util.*;
import java.lang.Exception;
import java.lang.Integer;

public class ReadLog {
    public ReadLog() {}

    public static void getSortedKeys
        (String owner, String jobNo,
         List<String> sKeys, Map<String,String> mStat) {
        String base = "/astro/net/pogo3/rgibson/testFramework011312/";
        String inFile = new String(base);
        inFile += "dumpJobDB" + owner + "_" + jobNo + ".dat";
        System.out.println("Will read from:" + inFile);
        List<String> lines = new ArrayList<String>();

        try {
            // Open the file that is the first 
            // command line parameter
            FileInputStream fStream = new FileInputStream(inFile);

            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fStream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;

            //Read File Line By Line
            while ((strLine = br.readLine()) != null)   {
                // Print the content on the console
                //System.out.println (strLine);
                lines.add(strLine);
            }
            in.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        //System.out.println("-----------------------------------");
        List<String> sList = sortJobsInStringList(lines, owner);
        //Iterator<String> it = sList.iterator();
        //while (it.hasNext()) {
        //    System.out.println(it.next());
        //}

        // Create a sorted list of keys and a map to the status for each
        parseSortedList(sList, sKeys, mStat);
    }

    // Sorting is tricky because we want to sort on a substring
    private static List<String> sortJobsInStringList
        (List<String> s, String owner) {
        int i;
        List<String> outList = new ArrayList<String>();
        Map<Integer,String> m = new HashMap<Integer,String>();
        int minI = Integer.MAX_VALUE, maxI = -1;
        for (i = 0; i < s.size(); i++) {
            String s0 = new String(s.get(i));
            if (!s0.startsWith(owner)) continue;
            String[] s1Arr = s0.split(",");
            String[] s2Arr = s1Arr[0].split("_");
            if (s2Arr.length != 3) continue;
            Integer k = Integer.valueOf(s2Arr[2]);
            m.put(k, s0);
            if (k < minI) minI = k.intValue();
            if (k > maxI) maxI = k.intValue();
        }

        if (maxI < minI) {
            System.err.println("*** No jobs found; skipping.");
            return outList;
        }

        if (maxI - minI > 100000) {
            System.err.println("*** Too many jobs; skipping.");
            return outList;
        }

        for (i = minI; i <= maxI; i++) {
            Integer k = Integer.valueOf(i);
            if (!m.containsKey(k)) continue;
            outList.add(m.get(k));
        }
        return outList;
    }

    // Create a sorted list of keys and a map to the status for each
    private static void parseSortedList
        (List<String> sList, List<String> sKeys, Map<String,String> m) {
        int i;
        for (i = 0; i < sList.size(); i++) {
            String s0 = new String(sList.get(i));
            String[] s1Arr = s0.split(",");
            sKeys.add(s1Arr[0]);
            m.put(s1Arr[0], s1Arr[1]);
        }
    }

    // Split input into sorted lists of Queued, Running, and Finished
    public static DisplayLists splitSortedList
        (List<String> sKeys, Map<String,String> m) {
        int i;
        String k, m0, status;
        String[] s0Arr;
        DisplayLists dLs = new DisplayLists();
        for (i = 0; i < sKeys.size(); i++) {
            k = sKeys.get(i);
            m0 = new String(m.get(k));
            s0Arr = m0.split("_");
            status = s0Arr[0];
            if (status.equals("JobAdded")) {
                dLs.QueuedObsIds.add(s0Arr[1]);
            }
            else if (status.equals("JobRunning")) {
                dLs.RunningObsIds.add(s0Arr[1]);
            }
            else if (status.equals("JobRetry")) {
                if (s0Arr.length <= 2) {
                    dLs.RetriedObsIds.add(s0Arr[1]);
                }
                else {
                    dLs.RetriedObsIds.add
                        (s0Arr[1] + " (" + s0Arr[2] + ")");
                }
            }
            else if (status.equals("JobFinished")) {
                if (s0Arr.length <= 2) {
                    dLs.FinishedObsIds.add(s0Arr[1]);
                }
                else {
                    dLs.FinishedObsIds.add
                        (s0Arr[1] + " (" + s0Arr[2] + ")");
                    if (s0Arr.length == 4) {
                        dLs.JobRunTimes.add(Integer.decode(s0Arr[3]));
                    }
                }
            }
            else if (status.equals("JobFailedAndRemoved")) {
                if (s0Arr.length <= 2) {
                    dLs.FailedObsIds.add(s0Arr[1]);
                }
                else {
                    dLs.FailedObsIds.add
                        (s0Arr[1] + " (" + s0Arr[2] + ")");
                }
            }
            else {
                System.out.println("*** Unknown status: " + status);
                return null;
            }
        }
        return dLs;
    }

    public static void getInFileList(List<String> inList) {
        String base = "/astro/net/pogo3/rgibson/testFramework011312/";
        String inFile = new String(base);
        inFile += "inList.txt";
        System.out.println("Will read obsIds from:" + inFile);

        try {
            // Open the file that is the first 
            // command line parameter
            FileInputStream fStream = new FileInputStream(inFile);

            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fStream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;

            //Read File Line By Line
            while ((strLine = br.readLine()) != null)   {
                // Print the content on the console
                //System.out.println (strLine);
                inList.add(strLine);
            }
            in.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
