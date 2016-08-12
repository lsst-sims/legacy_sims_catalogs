// Based on http://www.coderanch.com/t/344345/GUI/java/Simple-Graph

//import java.awt.*;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.lang.Math;
import java.util.*;
import javax.swing.*;
 
public class PlotHist extends JPanel {
    int[] Data = { 25, 60, 42, 75 };
    final int Pad = 50;
    String xLabel = new String("Log10(Completion Time in sec)");
    String yLabel = new String("Number");
    double[] BLo = null;
    double[] BHi = null;
    int[] DataHist = null;
 
    private static Font LabelFont = new Font("SanSerif", Font.BOLD, 18);
    public void setData(int[] d) { Data = d; }

    protected void paintComponent(Graphics g) {
        int i;
        super.paintComponent(g);
        int t0, t1;
        double d0, d1, d2;
        Graphics2D g2 = (Graphics2D)g;
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
                            RenderingHints.VALUE_ANTIALIAS_ON);
        int w = getWidth();
        int h = getHeight();

        // The origin location.
        int x0 = Pad;
        int y0 = h-Pad;

        g2.drawLine(Pad, 0, Pad, h-Pad);
        g2.drawLine(Pad, h-Pad, w-Pad, h-Pad);
        g2.drawLine(w-Pad, h-Pad, w-Pad, 0);
        g2.drawLine(Pad, 0, w-Pad, 0);


        //double xScale = (w - 2*Pad)/(Data.length + 1);
        //double maxValue = 100.0;
        //double yScale = (h - 2*Pad)/maxValue;
        g2.setPaint(Color.red);
        //for(int j = 0; j < Data.length; j++) {
        //    int x = x0 + (int)(xScale * (j+1));
        //    int y = y0 - (int)(yScale * Data[j]);
        //    g2.fillOval(x-2, y-2, 4, 4);
        //}
        if (DataHist != null) {
            double xScale = (w-2.*Pad) / (BHi[BHi.length-1]-BLo[0]);
            int maxVal = -1;
            for (i = 0; i < BLo.length; i++) {
                if (DataHist[i] > maxVal) maxVal = DataHist[i];
            }
            double yScale = (1.0*h-Pad) / maxVal;
            for (i = 0; i < BLo.length; i++) {
                if (DataHist[i] == 0) continue;
                d0 = Pad + (BLo[i]-BLo[0])*xScale;
                d1 = Pad + (BHi[i]-BLo[0])*xScale;
                g.setColor(Color.green);
                int x1 = (int) (d0+0.5);
                int y1Base = h-Pad;
                int x1W = (int) (d1-d0+0.5);
                int dy = (int) (DataHist[i]*yScale+0.5);
                int y1 = y1Base - dy;
                int y1H = -dy;
                System.out.println("DH[" + i + "]: " + DataHist[i]);
                System.out.println("Box: " + x1 + " " + y1 + " " + x1W + " -" + y1H);
                g.fillRect(x1, y1, x1W, -y1H);
            }
            DrawXAxisScale(xScale, (int) (h-0.7*Pad), w, g);
            DrawYAxisScale(yScale, (int) (0.5*Pad), h, maxVal, g);
        }
        
        if (xLabel != null) DrawXLabel(xLabel, g);
        if (yLabel != null) DrawYLabel(yLabel, g);
    }

    private void DrawXLabel(String s, Graphics g) {
        g.setFont(LabelFont);
        g.setColor(Color.black);
        int r = getHeight() - (int) (0.20*Pad);
        DrawCenteredHorizString(s, r, g);
    }

    private void DrawCenteredHorizString(String s, int r, Graphics g) {
        int w = getWidth();
        int h = getHeight();
        FontMetrics fm = g.getFontMetrics();
        int x = (w - fm.stringWidth(s)) / 2;
        //int y = (fm.getAscent() + (h - (fm.getAscent()+fm.getDescent()))/2.);
        g.drawString(s, x, r);
    }
 
    private void DrawYLabel(String s, Graphics g) {
        g.setFont(LabelFont);
        g.setColor(Color.black);
        int c = (int) (0.4*Pad);
        DrawCenteredVerticalString(s, c, g);
    }

    private void DrawCenteredVerticalString(String s, int c, Graphics g) {
        int w = getWidth();
        int h = getHeight();
        FontMetrics fm = g.getFontMetrics();
        int y = (h - fm.stringWidth(s)) / 2;
        //int y = (fm.getAscent() + (h - (fm.getAscent()+fm.getDescent()))/2.);
        
        AffineTransform fontAT = new AffineTransform();
        Font curFont = g.getFont();
        
        // Derive a new, rotated font
        fontAT.rotate(-Math.PI / 2.);
        Font derivedFont = curFont.deriveFont(fontAT);
        g.setFont(derivedFont);
        //System.out.println("y is:" + y);
        g.drawString(s, c, h-y);
        g.setFont(curFont);
    }

    public int[] MakeHistogram(double[] bLo, double[] bHi, List<Integer> x) {
        int i, j;
        int[] h = new int[bLo.length];
        for (i = 0; i < h.length; i++) h[i] = 0;

        for (i = 0; i < x.size(); i++) {
            for (j = 0; j < bLo.length; j++) {
                int x0 = x.get(i).intValue();
                if (x0 >= bLo[j] && x0 <= bHi[j]) {
                    h[j]++;
                    break;
                }
            }
        }
        return h;
    }

    public void BuildTimeHistogram(List<Integer> x) {
        // Create some logarithmic time bins
        double[] bLo = {-1, 0, 1, 2, 3, 4, 5};
        double[] bHi = {0, 1, 2, 3, 4, 5, 6};
        int[] h = MakeHistogram(bLo, bHi, x);
        BLo = bLo; BHi = bHi; DataHist = h;
        //for (int i = 0; i < h.length; i++) System.out.println("i: " + i + " h[i]: " + h[i]);
    }

    private void DrawXAxisScale(double xScale, int r, int width, Graphics g) {
        int i, i0;
        g.setColor(Color.black);
        for (i = 0; i < BLo.length; i++) {
            // Subtract off a small fraction of width to center
            i0 = (int) (Pad + (BLo[i]-BLo[0])*xScale - 0.02*width);
            g.drawString(Double.toString(BLo[i]), i0, r);
        }
        double d1 = BHi[BHi.length-1];
        i0 = (int) (Pad + (d1-BLo[0])*xScale);
        g.drawString(Double.toString(d1), i0, r);
    }

    private void DrawYAxisScale
        (double yScale, int c, int height, int maxVal, Graphics g) {
        /*
        int i, i0;
        g.setColor(Color.black);
        for (i = 0; i < BLo.length; i++) {
            // Subtract off a small fraction of width to center
            i0 = (int) (Pad + (BLo[i]-BLo[0])*xScale - 0.02*width);
            g.drawString(Double.toString(BLo[i]), i0, r);
        }
        double d1 = BHi[BHi.length-1];
        i0 = (int) (Pad + (d1-BLo[0])*xScale);
        g.drawString(Double.toString(d1), i0, r);
        */
    }
}

