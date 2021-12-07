package com.bigdata.springboot.es2rocketmq;

import com.bigdata.springboot.bean.ElasticSearchHelper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

public class ElasticSearchApiTests {

    private String host = "192.168.1.104";
    private int port1 = 9200;
    private int port2 = 9201;

    private ElasticSearchHelper esHelper;

    @BeforeClass
    public void Setup() {
        try {
            esHelper = new ElasticSearchHelper(host, port1, port2);
        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
        }
    }

    @AfterClass
    public void Close() {
        esHelper.Close();
        esHelper = null;
    }

    @Test(groups = "groupCorrect")
    public void CreatedIndex(String index) {
        boolean successful = esHelper.CreateIndex(index);
        System.out.println("index created:" + successful);
        Assert.assertTrue(successful);
    }

    @Test(groups = "groupCorrect")
    public void DeleteIndex(String index) {
        boolean successful = esHelper.DeleteIndex(index);
        System.out.println("index deleted:" + successful);
        Assert.assertTrue(successful);
    }

    @Test(groups = "groupCorrect")
    public void InsertText(String index, String content) {
        boolean successful = esHelper.InsertTextData(index, content);
        System.out.println("content inserted:" + successful);
        Assert.assertTrue(successful);
    }

    @Test(groups = "groupCorrect")
    public void InsertText(String index, String id, String content) {
        boolean successful = esHelper.InsertTextData(index, id, content);
        System.out.println("content inserted:" + successful);
        Assert.assertTrue(successful);
    }

    @Test(groups = "groupCorrect")
    public void GetDataByIndex() {
        List<String> dataList = esHelper.GetDataBySearch("kafka");
        //String data = esHelper.GetData("kafka");
        String data = String.join(",", dataList);
        System.out.println("content :" + data);
        Assert.assertNotNull(data);
    }
}
