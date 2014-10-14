package framework.driven;

import junit.framework.Assert;
import main.framework.Drive.TaskRepository;
import org.dom4j.Node;
import org.junit.Test;

import java.util.HashMap;

/**
 * Created by adam on 14-2-19.
 */
public class NodeRepoTest {

    @Test
    public void testFlyWeight(){
        HashMap<String,Node> nodeRepo = TaskRepository.getNodeRepo();
        Assert.assertEquals(nodeRepo.size(),0);
        Node defClean = TaskRepository.getNodeByName("defClean");
        Assert.assertEquals(nodeRepo.size(),1);
        String workerName = TaskRepository.getNodeAtrr(defClean, "worker");
        Assert.assertEquals(workerName,"main.framework.Work.DefaultImpl.DefaultCleaner");
        Node defToken = TaskRepository.getNodeByName("defToken");
        Assert.assertEquals(nodeRepo.size(),2);
    }
}
