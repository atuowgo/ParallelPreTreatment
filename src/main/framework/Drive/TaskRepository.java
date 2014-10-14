package main.framework.Drive;

import main.framework.Resource.ResourceInfo;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.util.HashMap;

/**
 * Created by adam on 14-2-19.
 */
public class TaskRepository {
    private static HashMap<String,Node> nodeRepo;
    private static Document doc;

    static {
        nodeRepo = new HashMap<String, Node>();
        SAXReader reader = new SAXReader();
        try {
            doc = reader.read(ResourceInfo.class.getResource("def-taskrep.xml").getPath());
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    private TaskRepository(){}

    public static Node getNodeByName(String nodeName){
        if (nodeRepo.keySet().contains(nodeName))
            return nodeRepo.get(nodeName);
        else {
            Node node = getNodeFromDoc(nodeName);
            nodeRepo.put(nodeName,node);
            return node;
        }
    }

    public static Node getNodeFromDoc(String nodeName){
        Node node = doc.selectSingleNode("//task[@name='"+nodeName+"']");
        return node;
    }

    public static HashMap<String, Node> getNodeRepo() {
        return nodeRepo;
    }

    public static Document getDoc() {
        return doc;
    }

    public static String getNodeAtrr(Node taskNode,String attr){
        return taskNode.selectSingleNode("./@"+attr).getText().trim();
    }
}
