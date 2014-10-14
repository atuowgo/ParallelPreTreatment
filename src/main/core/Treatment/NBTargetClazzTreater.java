package main.core.Treatment;

import main.core.Model.NB.DefNBAggreateModel;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.util.Iterator;

/**
 * Created by adam on 14-3-23.
 */
public class NBTargetClazzTreater {

    /**
     * @param srcVector       代分类的文档的向量模型
     * @param model     每个类的向量分类器模型
     * @return 成功分类的类标号
     */
    public static int findTargetClazz(Vector srcVector, DefNBAggreateModel model) {
        Vector[] NBModelList = model.getNBModelList();
        double[] percentageClazz = model.getPercentageList();
        int clazzNum = model.getClazzNum();

        if (NBModelList.length != percentageClazz.length)
            return Integer.MIN_VALUE;

//        存放每个类的模型计算值
        Vector clazzNB = new DenseVector(clazzNum);
        double[] sumNB = new double[clazzNum];
        Iterator<Vector.Element> iterVector = srcVector.nonZeroes().iterator();
        Vector.Element e = null;

        int eIndex = -1;
        double value = -1;

//        for(int i=0;i<clazzNum;i++)
//            NBModelList[i] = NBModelList[i];

        while (iterVector.hasNext()) {
            e = iterVector.next();
            eIndex = e.index();

            for (int i = 0; i < clazzNum; i++) {
                value = NBModelList[i].get(eIndex);
//                if (value!= 0)
//                    System.out.println("i : "+value+" : "+Math.log(value));
                    sumNB[i] += Math.log(value)*e.get();
//                sumNB[i] += Math.log(value);
//                权重不是很好，最好换为tfidf
            }
        }


        for (int i = 0; i < clazzNum; i++) {
            sumNB[i] += Math.log(percentageClazz[i]);
            clazzNB.set(i, sumNB[i]);
        }
//        System.out.println(clazzNB);

        int maxIndex = clazzNB.maxValueIndex();
//        System.out.println(maxIndex);
        return maxIndex;
//        return LabelProUtil.parseLabelStr(maxIndex);
    }
}
