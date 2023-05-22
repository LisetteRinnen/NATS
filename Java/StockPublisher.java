import io.nats.client.*;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * Take the NATS URL on the command-line.
 */
public class StockPublisher {

  private static String natsURL = "nats://localhost:4222";
  public static void main(String... args) throws Exception {
      if (args.length > 0) {
          natsURL = args[0];
      }

      System.console().writer().println("Starting stock publisher....");

      StockMarket sm1 = new StockMarket(StockPublisher::publishMessage, "AMZN", "MSFT", "GOOG", "APPL", "NVDA", "TSLA", "ADBE", "INTC");
      new Thread(sm1).start();
      StockMarket sm2 = new StockMarket(StockPublisher::publishMessage, "ACTV", "BLIZ", "ROVIO", "FDX", "MNST", "COF", "ROST", "DLTR", "KHC");
      new Thread(sm2).start();
      StockMarket sm3 = new StockMarket(StockPublisher::publishMessage, "GE", "GMC", "F", "CAT", "TSLA", "RACE", "TM", "MBGYY", "HMC", "VWAGY");
      new Thread(sm3).start();
    }

    public synchronized static void publishDebugOutput(String symbol, int adjustment, int price) {
        System.console().writer().printf("PUBLISHING %s: %d -> %f\n", symbol, adjustment, (price / 100.f));
    }
    // When you have the NATS code here to publish a message, put "publishMessage" in
    // the above where "publishDebugOutput" currently is
    public synchronized static void publishMessage(String symbol, int adjustment, int price) {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        String msg = buildXMLString(timeStamp, symbol, adjustment, price);
        try (Connection nc = Nats.connect(natsURL)) {
            nc.publish("PriceAdjustment." + symbol, msg.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String buildXMLString(String timeStamp, String symbol, int adjustment, int price) {
        StringBuilder sb = new StringBuilder();
        sb.append("<message sent=\"").append(timeStamp).append("\">\n");
        sb.append("  <stock>\n");
        sb.append("    <name>").append(symbol).append("</name>\n");
        sb.append("    <adjustment>").append(adjustment).append("</adjustment>\n");
        sb.append("    <adjustedPrice>").append(price).append("</adjustedPrice>\n");
        sb.append("  </stock>\n");
        sb.append("</message>");
        return sb.toString();
    }
}