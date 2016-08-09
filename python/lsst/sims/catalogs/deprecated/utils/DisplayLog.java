import java.util.*;

import java.awt.Color;
import java.awt.ComponentOrientation;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.lang.Integer;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.UIManager;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JViewport;
import javax.swing.border.Border;
import javax.swing.BorderFactory;

public class DisplayLog extends JFrame implements ActionListener, ItemListener {
    DisplayLists DLs;
    Timer TheTimer;
    static final int TickInterval = 5000;
    int TimerTickNo;
    List<JTextArea> TAList;
    List<JScrollPane> SPList;
    List<JLabel> HeaderList;
    String Owner;
    String JobNo;
    JCheckBox AutoRefresh;
    JButton RefreshNow;
    boolean AutoRefreshFlag = true;
    PlotHist PlotHistPanel;

    static final int ListInFileI = 0;
    static final int ListUnqueuedI = 1;
    static final int ListQueuedI = 2;
    static final int ListRunningI = 3;
    static final int ListRetriedI = 4;
    static final int ListFinishedI = 5;
    static final int ListFailedI = 6;
    static final int NPanes = 7;

    static final Color[] TextBGColors = {
        UIManager.getColor("TextField.background"),
        UIManager.getColor("TextField.background"),
        UIManager.getColor("TextField.background"),
        Color.GREEN,
        Color.YELLOW,
        Color.GREEN,
        Color.RED,
        Color.RED
    };

    static final String[] Headers = { "Input", "Unqed", "Queued",
                                "Running", "Retried", "Finished",
                                "Failed" };

    public DisplayLog(String owner, String jobNo) {
        // First, load up the data structures.  Do this *before*
        //  firing off the UI so the initial data is in place.
        Owner = owner;
        JobNo = jobNo;
        updateData();
        
        // Also fill in the input list
        ReadLog.getInFileList(DLs.InputObsIds);

        TimerTickNo = -1;
        TheTimer = new Timer(TickInterval, this);
        TheTimer.start();

        initUI();
    }

    public final void initUI() {
        int i;
        JPanel containerPanel = new JPanel();
        containerPanel.setBorder
            (BorderFactory.createEmptyBorder(5, 5, 5, 5));
        containerPanel.setLayout
            (new BoxLayout(containerPanel, BoxLayout.Y_AXIS));

        JPanel panel = new JPanel();
        panel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));

        // Do the scroll panes before the headers so we can get
        //  the width of each
        JPanel listPanel = new JPanel();
        listPanel.setAlignmentX(1f);
        listPanel.setLayout(new BoxLayout(listPanel, BoxLayout.X_AXIS));
        TAList = new ArrayList<JTextArea>();
        for (i = 0; i < NPanes; i++) {
            JTextArea jTA = new JTextArea(0, 8);
            jTA.setLineWrap(false);
            // Poor man's method of right-justification, which doesn't
            //  seem to be generally provided for JTextArea objects
            jTA.setComponentOrientation(ComponentOrientation.RIGHT_TO_LEFT);
            TAList.add(jTA);
        }

        SPList = new ArrayList<JScrollPane>();
        HeaderList = new ArrayList<JLabel>();
        for (i = 0; i < NPanes; i++) {
            JScrollPane jSP = new JScrollPane(TAList.get(i));
            jSP.setVerticalScrollBarPolicy
                (JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
            JViewport j0 = new JViewport();
            JLabel lab0 = new JLabel(Headers[i], JLabel.CENTER);
            lab0.setOpaque(true);
            lab0.setBackground(TextBGColors[i]);
            Border b0 = BorderFactory.createLineBorder(Color.BLACK);
            lab0.setBorder(b0);
            HeaderList.add(lab0);
            j0.setView(lab0);
            jSP.setColumnHeader(j0);
            listPanel.add(jSP);
            SPList.add(jSP);
        }

        JPanel controlPanel = new JPanel();
        controlPanel.setAlignmentX(1f);
        controlPanel.setLayout(new BoxLayout(controlPanel, BoxLayout.X_AXIS));
        AutoRefresh = new JCheckBox("Auto Refresh");
        AutoRefresh.setMnemonic(KeyEvent.VK_A);
        AutoRefresh.setSelected(AutoRefreshFlag);
        AutoRefresh.addItemListener(this);
        controlPanel.add(AutoRefresh);
        controlPanel.add(Box.createRigidArea(new Dimension(20, 0)));
        RefreshNow = new JButton("Refresh Now");
        RefreshNow.addActionListener(this);
        controlPanel.add(RefreshNow);

        panel.add(listPanel);
        panel.add(controlPanel);
        panel.setPreferredSize(new Dimension(800, 250));
        containerPanel.add(panel);

        // Create the histogram panel
        PlotHistPanel = new PlotHist();
        PlotHistPanel.setPreferredSize(new Dimension(800, 200));
        containerPanel.add(PlotHistPanel);
        add(containerPanel);

        setTitle("DisplayLog");
        setSize(800, 500);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        // Show information as soon as UI is visible
        updateUI();
    }

    public static void main(String args[]) {
        final String owner = args[0];
        final String jobNo = args[1];
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                DisplayLog ex = new DisplayLog(owner, jobNo);
                ex.setVisible(true);
            }
        });
    }

    public void actionPerformed(ActionEvent e) {
        int i;
        Object src = e.getSource();
        if (src == TheTimer) {
            TimerTickNo += 1;
            System.out.println(">>> Timer tick: " + TimerTickNo);
            if (AutoRefreshFlag == true) {
                updateData();
                updateUI();
            }
        }
        else if (src == RefreshNow) {
            updateData();
            updateUI();
        }
        else {
            System.out.println("*** Unknown action source.");
        }
    }

    public void itemStateChanged(ItemEvent e) {
        System.out.println("*** itemStateChanged().");
        Object src = e.getSource();
        if (src == AutoRefresh) {
            AutoRefreshFlag = AutoRefresh.isSelected();
        }
        else {
            System.out.println("*** Unknown source.");
        }
    }

    public void updateData() {
        // Update the list of dynamically changing data
        System.out.println("Update Data.");
        List<String> sKeys = new ArrayList<String>();
        Map<String,String> m = new HashMap<String,String>();
        ReadLog.getSortedKeys(Owner, JobNo, sKeys, m);
        DLs = ReadLog.splitSortedList(sKeys, m);
    }

    public void updateUI() {
        int i, j;
        JTextArea curText;
        JLabel lab0;
        System.out.println("Update UI.");
        List<String> list0;
        for (i = 0; i < NPanes; i++) {
            curText = TAList.get(i);
            curText.setText("");
            curText.setRows(0);
            if (i == 1) list0 = findOnlyInInputList();
            else list0 = DLs.AllLists.get(i);
            for (j = 0; j < list0.size(); j++) {
                curText.append(list0.get(j));
                if (j < list0.size() - 1) {
                    curText.append("\n");
                }
            }
            lab0 = HeaderList.get(i);
            lab0.setText(Headers[i] + " (" + list0.size() + ")");
            curText.setEditable(false);
            curText.setBackground(TextBGColors[i]);
        }
        PlotHistPanel.BuildTimeHistogram(DLs.JobRunTimes);
    }

    public List<String> findOnlyInInputList() {
        int i, j, k;
        List<String> in0 = new ArrayList<String>();
        // Make a copy of only the ones not in other lists
        String foundStr = "INVALID";
        for (i = 0; i < DLs.InputObsIds.size(); i++) {
            String s0 = DLs.InputObsIds.get(i);
            //System.out.println("Searching for: " + s0);
            boolean foundIt = false;
            foundStr = "INVALID";
            for (j = 2; j < NPanes && foundIt == false; j++) {
                List<String> list0 = DLs.AllLists.get(j);
                for (k = 0; k < list0.size() && foundIt == false; k++) {
                    String s1 = list0.get(k);
                    String[] s1Arr = s1.split(" ");
                    if (s1Arr.length > 0 && s0.equals(s1Arr[0])) {
                        //System.out.println("   Found in: " + j);
                        foundIt = true;
                        foundStr = s1Arr[0];
                        break;
                    }
                }
            }
            // If this does not show in any other list, then
            //  it is unique
            if (foundIt == false) {
                //System.out.println("   Adding: " + foundStr);
                in0.add(s0);
            }
        }
        return in0;
    }
}
