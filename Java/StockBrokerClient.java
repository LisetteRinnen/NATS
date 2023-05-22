import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

/**
 * Usage: java StockBrokerClient [client_name] [stockbroker_name] [nats_url] [path_to_strategy.xml] [portfolio.xml]
 */
public class StockBrokerClient {
    private static final String PRICE_ADJUSTMENT_SUBJECT = "PriceAdjustment.*";
    private static final String ORDER_SUBJECT_PREFIX = "Order.";
    private static String ORDER_SUBJECT;

    public static void main(String[] args) {
        ORDER_SUBJECT = ORDER_SUBJECT_PREFIX + args[1]; // broker name
        ORDER_SUBJECT += "." + args[0]; // client name
        String natsURL = (args.length > 2 && args[2] != "") ? args[2] : "nats://127.0.0.1:4222";
        String strategyPath = (args.length > 3 && args[3] != "") ? args[3] : "strategy-1.xml";
        String portfolioPath = (args.length > 4 && args[4] != "") ? args[4] : "portfolio-1.xml";

        try {
            Portfolio portfolio = new Portfolio(portfolioPath);
            Strategy strategy = new Strategy(strategyPath, portfolio);
            System.out.println(strategy.toString());

            Connection nc = Nats.connect(natsURL);
            final Connection conn = nc; // prevent compiler complaining about non-final variable used in lambda
            
            Dispatcher priceAdjustSubDispatcher = nc.createDispatcher((msg) -> {
                try {
                    handlePriceAdjust(msg, strategy, portfolio, conn);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            });
            priceAdjustSubDispatcher.subscribe(PRICE_ADJUSTMENT_SUBJECT);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void handlePriceAdjust(Message msg, Strategy strategy, Portfolio portfolio, Connection nc) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream msgStream = new ByteArrayInputStream(msg.getData());
        Document msgDoc = docBuilder.parse(msgStream);

        NodeList stocks = msgDoc.getElementsByTagName("stock");
        StockPrice[] stockPrices = new StockPrice[stocks.getLength()];
        for (int i = 0; i < stocks.getLength(); i++) {
            Element stock = (Element) stocks.item(i);

            String symbol = stock.getElementsByTagName("name")
                .item(0)
                .getTextContent();
            String adjustedPriceCents = stock.getElementsByTagName("adjustedPrice")
                .item(0)
                .getTextContent();

            stockPrices[i] = new StockPrice(symbol, Integer.parseInt(adjustedPriceCents));
        }

        final Connection conn = nc; // prevent compiler complaining about non-final variable used in lambda
        strategy.evaluate(stockPrices, txn -> { try {
            requestTransaction(txn, portfolio, conn);
        } catch (Exception e) {
            e.printStackTrace();
        } });
    }

    private static void requestTransaction(StockTransaction transaction, Portfolio portfolio, Connection nc) throws InterruptedException, ExecutionException, TimeoutException, ParserConfigurationException, SAXException, IOException, TransformerException {
        System.out.println(transaction.toString());
        String orderMsg = "<order><" +
            transaction.type +
            " symbol=\"" +
            transaction.symbol +
            "\" amount=\"" +
            transaction.shares +
            "\" /></order>";
        Future<Message> incoming = nc.request(ORDER_SUBJECT, orderMsg.getBytes(StandardCharsets.US_ASCII));
        Message msg = incoming.get(1000, TimeUnit.MILLISECONDS);
        handleOrderReceipt(msg, portfolio);
    }

    private static void handleOrderReceipt(Message msg, Portfolio portfolio) throws ParserConfigurationException, SAXException, IOException, TransformerException {
        DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream msgStream = new ByteArrayInputStream(msg.getData());
        Document msgDoc = docBuilder.parse(msgStream);

        NodeList sellElems = msgDoc.getElementsByTagName("sell");
        for (int i = 0; i < sellElems.getLength(); i++) {
            Element sell = (Element) sellElems.item(i);
            String symbol = sell.getAttribute("symbol");
            int amount = Integer.parseInt(sell.getAttribute("amount"));

            portfolio.setShares(
                symbol,
                portfolio.getShares(symbol) - amount
            );
        }

        NodeList buyElems = msgDoc.getElementsByTagName("buy");
        for (int i = 0; i < buyElems.getLength(); i++) {
            Element buy = (Element) buyElems.item(i);
            String symbol = buy.getAttribute("symbol");
            int amount = Integer.parseInt(buy.getAttribute("amount"));

            portfolio.setShares(
                symbol,
                portfolio.getShares(symbol) + amount
            );
        }
    }

    private static class Portfolio {
        private Document document;
        private String path;
    
        public Portfolio(String portfolioPath) throws ParserConfigurationException, IOException, SAXException {
            path = portfolioPath;
            load();
        }
    
        public int getShares(String symbol) throws ParserConfigurationException, SAXException, IOException {
            load();
            Element portfolioElement = document.getDocumentElement();
            NodeList stockNodes = portfolioElement.getElementsByTagName("stock");
    
            for (int i = 0; i < stockNodes.getLength(); i++) {
                Node stockNode = stockNodes.item(i);
                if (stockNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element stockElement = (Element) stockNode;
                    String stockSymbol = stockElement.getAttribute("symbol");
                    if (stockSymbol.equals(symbol)) {
                        String sharesText = stockElement.getTextContent();
                        return Integer.parseInt(sharesText);
                    }
                }
            }
            return 0;
        }
    
        public void setShares(String symbol, int shares) throws TransformerException {
            Element portfolioElement = document.getDocumentElement();
            NodeList stockNodes = portfolioElement.getElementsByTagName("stock");
    
            for (int i = 0; i < stockNodes.getLength(); i++) {
                Node stockNode = stockNodes.item(i);
                if (stockNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element stockElement = (Element) stockNode;
                    String stockSymbol = stockElement.getAttribute("symbol");
                    if (stockSymbol.equals(symbol)) {
                        stockElement.setTextContent(Integer.toString(shares));
                        break;
                    }
                }
            }
            save();
        }

        private void load() throws ParserConfigurationException, SAXException, IOException {
            DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            document = docBuilder.parse(new File(path));
        }

        private void save() throws TransformerException {
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
    
            DOMSource source = new DOMSource(document);
            StreamResult result = new StreamResult(new File(path));
            transformer.transform(source, result);
        }
    }

    private static class Strategy {
        Rule[] rules;
        Portfolio portfolio;

        public Strategy(String strategyPath, Portfolio portfolio) throws ParserConfigurationException, IOException, SAXException {
            this.portfolio = portfolio;

            DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            File strategyFile = new File(strategyPath);
            Document strategy = docBuilder.parse(strategyFile);
            NodeList ruleElems = strategy.getElementsByTagName("when");

            rules = new Rule[ruleElems.getLength()];
            for (int i = 0; i < ruleElems.getLength(); i++) {
                Element ruleElem = (Element) ruleElems.item(i);
                rules[i] = new Rule(ruleElem);
            }
        }

        public void evaluate(StockPrice[] stockPrices, Consumer<StockTransaction> placeTransaction) throws ParserConfigurationException, SAXException, IOException {
            for (StockPrice stockPrice: stockPrices) {
                for (Rule rule : rules) {
                    if (rule.signalRaised(stockPrice)) {
                        placeTransaction.accept(new StockTransaction(
                            rule.transaction.symbol,
                            rule.transaction.type,
                            rule.transaction.shares == -1 ? this.portfolio.getShares(rule.transaction.symbol) : rule.transaction.shares
                        ));
                    }
                }
            }
        }

        public String toString() {
            String out = "Strategy with rules:\n";
            for (Rule rule : rules) {
                out += "\t" + rule.toString() + "\n";
            }
            return out;
        }
    }

    private static class Rule {
        private String symbol;
        private int above = -1; // -1 represents lack of ceiling
        private int below = -1; // -1 represents lack of floor

        public StockTransaction transaction;

        public Rule(Element rule) {
            // parse relevant symbol
            symbol = rule.getElementsByTagName("stock")
                .item(0)
                .getTextContent();

            // parse conditions
            try {
                above = Integer.parseInt(
                    rule.getElementsByTagName("above")
                        .item(0)
                        .getTextContent()
                    );
            } catch (NullPointerException e) {
                above = -1;
            }

            try {
                below = Integer.parseInt(
                    rule.getElementsByTagName("below")
                        .item(0)
                        .getTextContent()
                    );
            } catch (NullPointerException e) {
                below = -1;
            }
            
            // parse actions (buy and sell are considered mutually exclusive)
            NodeList buyElems = rule.getElementsByTagName("buy");
            if (buyElems.getLength() > 0) {
                int shares = Integer.parseInt(buyElems.item(0).getTextContent());
                transaction = new StockTransaction(symbol, "buy", shares);
            }

            NodeList sellElems = rule.getElementsByTagName("sell");
            if (sellElems.getLength() > 0) {
                try {
                    int shares = Integer.parseInt(sellElems.item(0).getTextContent());
                    transaction = new StockTransaction(symbol, "sell", shares);
                } catch (NumberFormatException e) {
                    transaction = new StockTransaction(symbol, "sell", -1); // sell all available shares
                }
            }
        }

        /**
         * Evaluate whether or not to place this rule's transaction given
         * the stock price. The trading signal is raised if the price meets
         * this rule's conditions.
         * @param price stock price
         * @return trading signal
         */
        public boolean signalRaised(StockPrice price) {
            if (price.symbol.equals(symbol)) {
                if (above == -1 || price.adjustedPriceCents > above) {
                    if (below == -1 || price.adjustedPriceCents < below) {
                        return true;
                    }
                }
            }
            return false;
        }

        public String toString() {
            String aboveStr = above == -1 ? "-inf" : String.valueOf(above);
            String belowStr = below == -1 ? "inf" : String.valueOf(below);
            return "If " + aboveStr + " < " + symbol + " < " + belowStr + " then " + transaction.toString();
        }
    }

    /**
     * Represents a buy or sell of some amount of a symbol's shares.
     * If the number of shares is -1, then sell all available shares
     */
    public static class StockTransaction {
        public String symbol;
        public String type; // either "buy" or "sell"
        public int shares;

        public StockTransaction(String symbol, String type, int shares) {
            this.symbol = symbol;
            this.type = type;
            this.shares = shares;
        }

        public String toString() {
            String out = type + " ";
            if (shares == -1) {
                out += "all";
            } else {
                out += String.valueOf(shares);
            }
            out += " shares";
            out += " of " + symbol;
            return out;
        }
    }

    /**
     * Represents a symbol's stock price in cents
     */
    private static class StockPrice {
        public String symbol;
        public int adjustedPriceCents;

        public StockPrice(String symbol, int adjustedPriceCents) {
            this.symbol = symbol;
            this.adjustedPriceCents = adjustedPriceCents;
        }

        public String toString() {
            return symbol + " " + String.valueOf(adjustedPriceCents);
        }
    }
}
