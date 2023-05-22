import io.nats.client.*;
import java.io.File;
import java.io.FileWriter;
import org.w3c.dom.Document;
import javax.xml.parsers.*;
import javax.xml.xpath.*;
import java.io.StringReader;
// import input source
import org.xml.sax.InputSource;

public class StockMonitor {

    private static DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

    public static void main(String[] args) {
        try {
            Connection nc = Nats.connect("nats://localhost:4222");
            String[] stock_list = { "*" };
            if (args.length > 0) {
                stock_list = args;
            }
            createLogFiles(stock_list);
            subscribeToStocks(nc, stock_list);
        } catch (Exception e) {
            e.printStackTrace();
        }    
    }

    private static void createLogFiles(String[] stock_list) {
        try {
            for (String stock : stock_list) {
                File stock_file = new File("./logs/" +stock + "-log.log");
                stock_file.createNewFile();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void logPriceAdjustment(Message msg, String stock_name) {
        try {
            String adjustment = new String(msg.getData());
            FileWriter stock_file = new FileWriter("./logs/" + stock_name + "-log.log", true);
            stock_file.append(parseXML(adjustment));
            stock_file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String parseXML(String adjustment) {
        try {
            Document document = dbf.newDocumentBuilder().parse(new InputSource(new StringReader(adjustment)));
            XPath xpath = XPathFactory.newInstance().newXPath();
            XPathExpression timestampExpr = xpath.compile("/message/@sent");
            XPathExpression symbolExpr = xpath.compile("/message/stock/name/text()");
            XPathExpression amountExpr = xpath.compile("/message/stock/adjustment/text()");
            XPathExpression adjustedPriceExpr = xpath.compile("/message/stock/adjustedPrice/text()");

            String symbol = (String) symbolExpr.evaluate(document, XPathConstants.STRING);
            String timestamp = (String) timestampExpr.evaluate(document, XPathConstants.STRING);
            String amount = (String) amountExpr.evaluate(document, XPathConstants.STRING);
            String adjustedPrice = (String) adjustedPriceExpr.evaluate(document, XPathConstants.STRING);

            return symbol + " " + timestamp + " " + amount + " " + adjustedPrice + "\n";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    private static void subscribeToStocks(Connection nc, String[] stock_list) {
        Dispatcher stockMonitorDispatcher = nc.createDispatcher((msg) -> {
            try {
                if (stock_list[0].equals("*")) {
                    logPriceAdjustment(msg, "*");
                } else {
                    String subject = msg.getSubject();
                    String stock_name = subject.substring(subject.indexOf(".") + 1);
                    logPriceAdjustment(msg, stock_name);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        for (String stock : stock_list) {
            stockMonitorDispatcher.subscribe("PriceAdjustment." + stock);
        }
    }
}
