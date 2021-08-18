package com.hyit.parser;

import com.google.common.collect.MapMaker;
import com.hyit.prediction.Predictions;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ConcurrentMap;

public class XMLParserConfiguration {


    public static ConcurrentMap<String,String> map = new MapMaker().concurrencyLevel(15).makeMap();


    private static ClassLoader classLoader;


    static{
        try {
            classLoader = Thread.currentThread().getContextClassLoader();
            if(classLoader == null){
                classLoader = XMLParserConfiguration.class.getClassLoader();
            }
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setIgnoringComments(true);
            factory.setXIncludeAware(true);
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = null;
            Element root = null;
            URL url = classLoader.getResource("config-site.xml");
            Predictions.checkArgument(url==null,"classLoader can not get files from root directory, url is %s",url);
            doc = (url.toString() == null)? builder.parse(url.openStream()) : builder.parse(url.openStream(),url.toString());
            if(root == null){
                Predictions.checkArgument(doc == null,"Document can not parse successfully from DocumentBuilder, Document is %s",doc);
                root = doc.getDocumentElement();
            }
            Predictions.checkArgument(!"configuration".equalsIgnoreCase(root.getTagName()),"bad conf file : top-level element not <configuration>, element is %s",root.getTagName());
            NodeList props = root.getChildNodes();
            for(int i = 0; i < props.getLength(); i++){
                Node propNode = props.item(i);
                if(!(propNode instanceof Element)) continue;
                Element prop = (Element)propNode;
                Predictions.checkArgument(!"property".equalsIgnoreCase(prop.getTagName()),"bad conf file : element not <property>, element is %s",prop.getTagName());
                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                for(int j = 0; j < fields.getLength(); j++){
                    Node fieldNode = fields.item(j);
                    if(!(fieldNode instanceof Element)) continue;
                    Element field = (Element)fieldNode;
                    if("name".equalsIgnoreCase(field.getTagName()) && field.hasChildNodes()){
                        attr = field.getFirstChild().getNodeValue();
                    }else if("value".equalsIgnoreCase(field.getTagName()) && field.hasChildNodes()){
                        value = field.getFirstChild().getNodeValue();
                    }
                }
                map.putIfAbsent(attr,value);
            }
        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
        }
    }

    private XMLParserConfiguration(){

    }

    public static void init(){

    }

}
