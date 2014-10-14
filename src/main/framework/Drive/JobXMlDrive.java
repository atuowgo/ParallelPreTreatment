package main.framework.Drive;

import main.core.Util.LogUtil;
import main.framework.JobRepo.JobRepo;
import main.framework.Options.Options;
import main.framework.Task.Task;
import main.framework.Worker.AbstractWorker;
import org.apache.commons.logging.Log;
import org.dom4j.*;
import org.dom4j.io.SAXReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by adam on 14-2-19.
 */
public class JobXMlDrive {
    private Document doc;
    private List<Node> confNodes;
    private static Log consoleLog = LogUtil.getConsoleLog();

    public JobXMlDrive() {
        String defPath = JobRepo.class.getResource("job-conf.xml").getPath();
        initFromFile(defPath);
    }

    public JobXMlDrive(String confPath) {
        initFromFile(confPath);
    }

    private void initFromFile(String confPath) {
        SAXReader reader = new SAXReader();
        try {
            doc = reader.read(confPath);
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    public void buildTaskTupe() {
        Node startNode = doc.selectSingleNode("//start");
        confNodes = new ArrayList<Node>();
        Node iterNode = startNode;
        String continueText = null;
        while (!"end".equals(continueText = iterNode.selectSingleNode("./@continueto").getText().trim())) {
            iterNode = doc.selectSingleNode("//task[@name='" + continueText + "']");
            confNodes.add(iterNode);
        }
    }

    public void startJobTupe()
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Task task;
        AbstractWorker worker;
        Options options;
        Node repoNode;

        String taskName;
        String runner;
        String workerName;
        String optionName;
//        List<Element> jobElementList  = new ArrayList<Element>();
//        List<Task> taskList = new ArrayList<Task>();
//        List<Worker> workerList = new ArrayList<Worker>();

//        int i=0;

        for (Node n : confNodes) {
            Element e = (Element) n;

            Iterator it = e.attributeIterator();
//            获得taskName
            taskName = n.selectSingleNode("./@name").getText().trim();
//            获得task节点
            repoNode = TaskRepository.getNodeByName(taskName);
            Element repoElement = (Element) repoNode;
//            jobElementList.add(i,repoElement);

            while (it.hasNext()) {
                Attribute attr = (Attribute) it.next();
                String attrName = attr.getName().trim();
                String attrValue = attr.getValue().trim();
                if ("".equals(attrValue) || "continueto".equals(attrName) || "name".equals(attrName)) {
                    continue;
                }else {
                    Attribute repoAttr = repoElement.attribute(attrName);
                    if (repoAttr==null){
                        consoleLog.error("错误的配置");
                        return;
                    }

                    repoAttr.setValue(attrValue);
                }
            }

//            获得自定义Task
            runner = TaskRepository.getNodeAtrr(repoNode, "runner");
            task = (Task) Class.forName(runner).newInstance();
//            获得自定义Worker
            workerName = TaskRepository.getNodeAtrr(repoNode, "worker");
            worker = (AbstractWorker) Class.forName(workerName).newInstance();
//            获得自定义Options
            optionName = TaskRepository.getNodeAtrr(repoNode, "optioner");
            options = (Options) Class.forName(optionName).newInstance();
//            初始化Worker
            worker.setOptions(options);
            worker.setTaskNode(repoNode);

//            workerList.add(i,worker);
//            taskList.add(i,task);
//            i++;

            consoleLog.info("task : " + taskName + " 开始执行");
            task.execute(worker);
//            System.out.println(repoNode);
            consoleLog.info("task : " + taskName + " 执行结束");
        }


    }
}
