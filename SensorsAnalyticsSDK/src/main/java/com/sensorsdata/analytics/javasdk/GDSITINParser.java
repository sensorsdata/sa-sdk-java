package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * 中航信 ITIN 日志解析模块
 * Created by zouyuhan on 8/9/16.
 */
public class GDSITINParser {

//  final static String SA_SERVER_URL = "http://test-ckh-zyh.cloud.sensorsdata.cn:8006/sa?token=de28ecf691865360";
  final static String SA_SERVER_URL = "http://travelsky.cloud.sensorsdata.cn:8006/sa?token=03a412aa3d3b53e6";

  abstract static class EventPropertyType<X extends Object> {
    abstract X parseTextContent(final String textContent);
  }

  static class EventPropertyString extends EventPropertyType<String> {
    @Override String parseTextContent(final String textContent) {
      return textContent.trim();
    }
  }

  static class EventPropertyDate extends EventPropertyType<Date> {

    EventPropertyDate(final String dateFormat) {
      this.dateFormat = new SimpleDateFormat(dateFormat);
    }

    @Override Date parseTextContent(String textContent) {
      if (textContent == null || textContent.length() < 1) {
        return null;
      }
      try {
        return dateFormat.parse(textContent);
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    }

    final DateFormat dateFormat;
  }

  static class EventPropertyNode {

    EventPropertyNode(final String xmlNodeTag, final int xmlNodeIndex) {
      this.xmlNodeTag = xmlNodeTag;
      this.xmlNodeIndex = xmlNodeIndex;
      this.subNode = null;
    }

    EventPropertyNode append(final String xmlNodeTag, final int xmlNodeIndex) {
      this.subNode = new EventPropertyNode(xmlNodeTag, xmlNodeIndex);
      return this.subNode;
    }

    final String xmlNodeTag;
    final int xmlNodeIndex;
    EventPropertyNode subNode;
  }

  static class EventPropertyPath {

    EventPropertyPath(final EventPropertyNode propertyPath, final String propertyName,
        final EventPropertyType propertyType) {
      this.propertyPath = propertyPath;
      this.propertyName = propertyName;
      this.propertyType = propertyType;
    }

    void dumpProperty(Document document, Map<String, Object> proeprties) {
      EventPropertyNode propertyNode = this.propertyPath;
      NodeList nodeList = document.getElementsByTagName(propertyNode.xmlNodeTag);

      while (nodeList != null && nodeList.getLength() > propertyNode.xmlNodeIndex) {
        Node node = nodeList.item(propertyNode.xmlNodeIndex);

        if (propertyNode.subNode == null) {
          proeprties.put(propertyName, propertyType.parseTextContent(node.getTextContent()));
          break;
        } else if (node.getNodeType() == Node.ELEMENT_NODE) {
          propertyNode = propertyNode.subNode;
          nodeList = ((Element)node).getElementsByTagName(propertyNode.xmlNodeTag);
        }
      }
    }

    final EventPropertyNode propertyPath;
    final String propertyName;
    final EventPropertyType propertyType;
  }

  static class ITINParser {

    static class TrackThreadData {
      static final ThreadLocal<SensorsAnalytics> sa = new ThreadLocal<SensorsAnalytics>();
      static final ThreadLocal<List<EventPropertyPath>> ep =
          new ThreadLocal<List<EventPropertyPath>>();

    }

    static class TrackThread extends Thread {

      public TrackThread(Runnable r) {
        super(r);
      }

      @Override
      public void run() {
//        TrackThreadData.sa
//            .set(new SensorsAnalytics(new SensorsAnalytics.DebugConsumer(SA_SERVER_URL, true)));
//        TrackThreadData.sa
//            .set(new SensorsAnalytics(new SensorsAnalytics.BatchConsumer(SA_SERVER_URL, 50)));
        String fileName = "travelsky_" + this.getId();
        try {
          TrackThreadData.sa.set(new SensorsAnalytics(new SensorsAnalytics.LoggingConsumer
              ("/Users/zouyuhan/Documents/" + fileName)));
        } catch (IOException e) {
          e.printStackTrace();
        }

        List<EventPropertyPath> eventPropertyPathes = new ArrayList<EventPropertyPath>();
        // 请求成功标记
        eventPropertyPathes.add(new EventPropertyPath(new EventPropertyNode("Flag", 0), "flag",
            new EventPropertyString()));
        // 电子客票号
//        eventPropertyPathes.add(new EventPropertyPath(
//            new EventPropertyNode("ITINERARY", 0).append("TICKETS", 0).append("TICKETINFO", 0)
//                .append("TKTN", 0), "tktn", new EventPropertyString()));
        // 航班出票时间
        eventPropertyPathes.add(new EventPropertyPath(
            new EventPropertyNode("ITINERARY", 0).append("TICKETS", 0).append("TICKETINFO", 0)
                .append("ISSUEDDATE", 0), "issued_date", new EventPropertyDate("ddMMMyy")));
        // 航空公司
        eventPropertyPathes.add(new EventPropertyPath(
            new EventPropertyNode("ITINERARY", 0).append("SEGMENTS", 0).append("SEGMENT", 0)
                .append("NORMAL", 0).append("AIRLINE", 0), "airline", new EventPropertyString()));
        // 航班号
        eventPropertyPathes.add(new EventPropertyPath(
            new EventPropertyNode("ITINERARY", 0).append("SEGMENTS", 0).append("SEGMENT", 0)
                .append("NORMAL", 0).append("FLIGHTNO", 0), "fight_no", new EventPropertyString()));
        // 航班第一航段起飞时间
        eventPropertyPathes.add(new EventPropertyPath(
            new EventPropertyNode("ITINERARY", 0).append("SEGMENTS", 0).append("SEGMENT", 0)
                .append("NORMAL", 0).append("DATE", 0), "departure_date", new EventPropertyString()));
        // 航班出发地
        eventPropertyPathes.add(new EventPropertyPath(
            new EventPropertyNode("ITINERARY", 0).append("SEGMENTS", 0).append("SEGMENT", 0)
                .append("NORMAL", 0).append("LEGS", 0).append("LEG", 0).append("ORIGIN", 0), "origin",
            new EventPropertyString()));
        // 航班目的地
        eventPropertyPathes.add(new EventPropertyPath(
            new EventPropertyNode("ITINERARY", 0).append("SEGMENTS", 0).append("SEGMENT", 0)
                .append("NORMAL", 0).append("LEGS", 0).append("LEG", 0).append("DEST", 0),
            "destination", new EventPropertyString()));
        // 错误原因
        eventPropertyPathes.add(new EventPropertyPath(
            new EventPropertyNode("ITINERARY", 0).append("TICKETS", 0).append("TICKETINFO", 0)
                .append("ErrorReason", 0), "error_reason", new EventPropertyString()));

        TrackThreadData.ep.set(eventPropertyPathes);

        super.run();
      }
    }

    static class FileParser implements Callable<Boolean> {

      FileParser(final String logFile) {
        this.logFile = logFile;
      }

      @Override public Boolean call() throws Exception {
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
          for (String line; (line = br.readLine()) != null; ++count) {
            processLine(line);
            if ((count % 10000) == 0) {
              System.out.println(String.format("File [%s] has been read %d lines.", logFile,
                  count));
            }
          }
        }
        return true;
      }

      void processLine(String logLine) {
        // XXX: 特殊处理一种日志格式错误
        logLine = logLine + "</ErrorReason></Response>";

        Matcher m = LOG_PATTERN.matcher(logLine);
        if (!m.matches()) {
          if (logLine.contains("call DETR_K end. Response")) {
            System.out.println(logLine);
          }
          return;
        }

        String distinctId = m.group(2);
        // XXX: 部分用户名以 !! 开头
        if (distinctId.startsWith("!!")) {
          distinctId = distinctId.substring(2);
        }

        Map<String, Object> eventProperties = new HashMap<String, Object>();
        // 时间时间
        try {
          eventProperties.put("$time", DATE_FORMAT.parse(m.group(1)));
        } catch (Exception e) {
          e.printStackTrace();
          return;
        }
        // 原始查询时间
        eventProperties.put("origin_time", m.group(1));
        // 证件类型
        eventProperties.put("id_type", m.group(3));
        // 证件ID
        eventProperties.put("id_value", m.group(4));
        // 系统
        eventProperties.put("system", m.group(5));
        // 语言
        eventProperties.put("language", m.group(6));
        // 客票类型
        eventProperties.put("ticket_type", m.group(7));
        // 用户名
        eventProperties.put("user_name", m.group(2));
        // 用户名前缀(除去最后一位)
        eventProperties.put("user_name_prefix", m.group(2).substring(0, m.group(2).length() - 1));

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = null;
        try {
          dBuilder = dbFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
          return;
        }
        String response = m.group(m.groupCount());

        try {
          Document doc =
              dBuilder.parse(new InputSource(new StringReader(response.replaceAll("\\x10", ""))));
          doc.getDocumentElement().normalize();

          for (EventPropertyPath propertyPath : TrackThreadData.ep.get()) {
            propertyPath.dumpProperty(doc, eventProperties);
          }
        } catch (SAXException e) {
          e.printStackTrace();
          System.out.println(logLine);
        } catch (IOException e) {
          e.printStackTrace();
          System.out.println(logLine);
        }

        try {
          TrackThreadData.sa.get().track(distinctId, "DETRK", eventProperties);
        } catch (InvalidArgumentException e) {
          e.printStackTrace();
        }
      }

      private final Pattern LOG_PATTERN = Pattern.compile(
          "^\\[\\]\\ (\\d{2}\\-\\d{2}\\-\\d{2}\\ \\d{2}\\:\\d{2}\\:\\d{2})\\ \\[\\d+\\].*?User:\\ "
              + "([!a-z0-9]+)\\ .*?ITIN\\:(TN|NI)/([0-9a-zA-Z]+),\\{SYSTEM=([A-Z0-9]+);LANGUAGE=([A-Z]+)"
              + ";ET=([A-Z]+)\\}.*?DETR_K\\ end\\.\\ Response: (<Response>.*?</Response>).*",
          Pattern.CASE_INSENSITIVE);
      private final DateFormat DATE_FORMAT = new SimpleDateFormat("yy-MM-dd hh:mm:ss");

      private final String logFile;
    }

    ITINParser() {
      this.logParserExecutor =
          new ThreadPoolExecutor(PARSER_THREAD_NUM, PARSER_THREAD_NUM, 0L, TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            public Thread newThread(Runnable r) {
              return new TrackThread(r);
            }
          });

      this.futures = new LinkedList<Future<?>>();
    }

    void process(final String logFile) throws IOException {
      futures.add(this.logParserExecutor.submit(new FileParser(logFile)));
    }

    void waitAll() {
      for (Future<?> future:futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
    }

    void destory() {
      this.logParserExecutor.shutdown();

      try {
        this.logParserExecutor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    private final static int PARSER_THREAD_NUM = 4;

    private final ThreadPoolExecutor logParserExecutor;

    private final Collection<Future<?>> futures;
  }

  public static void main(String[] args) throws Exception {
    ITINParser parser = new ITINParser();

    parser.process(
        "/Users/zouyuhan/Documents/航信/MaskedLogs/maskedItinFnl_A4.data.log.2016-07-26.utf8");
    parser.process(
        "/Users/zouyuhan/Documents/航信/MaskedLogs/maskedItinFnl_A4.data.log.2016-07-27.utf8");
    parser.process(
        "/Users/zouyuhan/Documents/航信/MaskedLogs/maskedItinFnl_A4.data.log.2016-07-28.utf8");
    parser.process(
        "/Users/zouyuhan/Documents/航信/MaskedLogs/maskedItinFnl_A4.data.log.2016-07-29.utf8");
    parser.process(
        "/Users/zouyuhan/Documents/航信/MaskedLogs/maskedItinFnl_A4.data.log.2016-07-30.utf8");
    parser.process(
        "/Users/zouyuhan/Documents/航信/MaskedLogs/maskedItinFnl_A4.data.log.2016-08-01.utf8");
    parser.process(
        "/Users/zouyuhan/Documents/航信/MaskedLogs/maskedItinFnl_A4.data.log.2016-08-02.utf8");
    parser.process(
        "/Users/zouyuhan/Documents/航信/MaskedLogs/maskedItinFnl_A4.data.log.2016-08-03.utf8");
    parser.process(
        "/Users/zouyuhan/Documents/航信/MaskedLogs/maskedItinFnl_A4.data.log.2016-08-04.utf8");

    parser.waitAll();
    parser.destory();
  }

}
