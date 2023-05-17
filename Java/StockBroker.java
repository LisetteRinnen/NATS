import java.io.*;
import java.time.Duration;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Dispatcher;
import io.nats.client.Message;

public class StockBroker {
  // private String name;
  private static Map<String, Integer> marketPrices = new HashMap<>();

  // public StockBroker(String name) {
  //   this.name = name;
  // }

  public static void main(String... args) {
    String natsURL = "nats://127.0.0.1:4222";
    if (args.length > 0) {
      natsURL = args[0];
    }

    try (Connection nc = Nats.connect(natsURL)) {
      Dispatcher market = nc.createDispatcher((msg) -> {
        try {
          updatePrices(msg);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });

      market.subscribe("PriceAdjustment");

      Dispatcher request = nc.createDispatcher((msg) -> {
        try {
          receiveOrder(nc, msg);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });

      request.subscribe("Order");

      // Test publishing updates from the StockPublisher (market updates)
      String marketChange =
        "<message><stock><name>DOG</name><adjustment>5</adjustment><adjustedPrice>3259</adjustedPrice></stock></message>";

      nc.publish("PriceAdjustment", marketChange.getBytes());

      // Test requests from the StockBrokerClient (orders)
      String order = "<order><buy symbol=\"DOG\" amount=\"5\" /></order>";
      Message response = nc.request("Order", order.getBytes(), Duration.ofSeconds(1));

      System.out.println("Response seen by StockBrokerClient:");
      System.out.println(new String(response.getData()));

    } catch (Exception e) {
      e.printStackTrace();
    }

  }


  private static void updatePrices(Message msg) throws Exception {
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = builder.parse(new ByteArrayInputStream(msg.getData()));
    XPath xPath = XPathFactory.newInstance().newXPath();

    Node nameElement = (Node) xPath.compile("/message/stock/name").evaluate(
      doc, XPathConstants.NODE);
    String stock = nameElement.getTextContent();

    Node priceElement = (Node) xPath.compile("/message/stock/adjustedPrice").evaluate(
      doc, XPathConstants.NODE);
    String price = priceElement.getTextContent();

    if (!marketPrices.containsKey(stock)) {
      marketPrices.put(stock, Integer.parseInt(price));
    } else {
      marketPrices.replace(stock, Integer.parseInt(price));
    }
  }


  private static void receiveOrder(Connection nc, Message msg) throws Exception {
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = builder.parse(new ByteArrayInputStream(msg.getData()));
    XPath xPath = XPathFactory.newInstance().newXPath();

    boolean transactionCheck = (boolean) xPath.compile("/order/buy").evaluate(
      doc, XPathConstants.BOOLEAN);

    String transactionType = "buy";

    if (!transactionCheck) {
      transactionType = "sell";
    }

    String stock = (String) xPath.compile("/order/" + transactionType + "/@symbol").evaluate(
      doc, XPathConstants.STRING);

    String amount = (String) xPath.compile("/order/" + transactionType + "/@amount").evaluate(
      doc, XPathConstants.STRING);

    String reply = processOrder(stock, amount, transactionCheck);

    nc.publish(msg.getReplyTo(), reply.getBytes());
  }


  private static String processOrder(String stock, String amount, boolean transactionType) {
    int price = marketPrices.get(stock);
    int cost = price * Integer.parseInt(amount);
    int transactionFee = cost / 10;
    int totalTransactionCost = 0;

    if (transactionType) {
      totalTransactionCost = cost + transactionFee;
      return buildReply("buy", stock, amount, totalTransactionCost);
    } else {
      totalTransactionCost = cost - transactionFee;
      return buildReply("sell", stock, amount, totalTransactionCost);
    }
  }


  private static String buildReply(String transactionType, String stock, String amount, int cost) {
    StringBuilder sb = new StringBuilder();

    sb.append("<orderReceipt>");
    sb.append("<" + transactionType + " symbol=\"" + stock + "\" amount=\"" + amount + "\" />");
    sb.append("<complete amount=\"" + cost + "\" />");
    sb.append("</orderReceipt>");

    return sb.toString();
  }
}
