package com.yanping.lambda;

import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Properties;

import com.amazonaws.services.lambda.runtime.Context;
import com.braintreegateway.BraintreeGateway;
import com.braintreegateway.WebhookNotification;
import com.braintreegateway.Transaction;

import com.extensis.lambda.AcceptWebhooks;
import com.extensis.lambda.LambdaService;
import com.extensis.lambda.RequestClass;
import com.extensis.lambda.ResponseClass;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
public class AcceptWebhooksTest {

    private final String testFileName = "test-candles.properties";
    private final String wrongFileName = "wrong-file.properties";
    private final String emptyFileName = "empty.properties";
    
    private final String URL = "database.url";
    private final String URL_VALUE = "jdbc:mysql://yz-s-117-1347-dbcluster-1kss5uq0lt7lq.cluster-ccccfxwdmniv.us-west-2.rds.amazonaws.com:3306/yanpingData";
    private final String USER_NAME = "database.username";
    private final String USER_NAME_VALUE = "yanping";
    private final String PASSWORD = "database.password";
    private final String PASSWORD_VALUE = "yanping";
    
    private final String DRIVER = "database.driverClassName";
    private final String DRIVER_VALUE = "com.mysql.jdbc.Driver";
    private final String BRAINTREE_ENVIROMENT = "braintreeEnvironment";
    private final String BRAINTREE_ENVIROMENT_VALUE = "sandbox";
    private final String BRAINTREE_MERCHANTID = "braintreeMerchantID";
    private final String BRAINTREE_MERCHANTID_VALUE = "99222gggg";
    private final String BRAINTREE_PUBLIC_KEY = "braintreePublicKey";
    private final String BRAINTREE_PUBLIC_KEY_VALUE = "3xudgfj";
    private final String BRAINTREE_PRIVATE_KEY = "braintreePrivateKey";
    private final String BRAINTREE_PRIVATE_KEY_VALUE = "b36666666cfff9999999995";
    
    private static RequestClass invalidWebhook;
    private static RequestClass notHandleWebhook;
    private final static String INVALID_SIGNATURE = "invalidSignature";
    private final static String INVALID_PAYLOAD = "invalidPayload"; 
    
    private final static String BRAINTREE_ID ="fz46xr";
    private final static String BRAINTREE_TRANSACTION_ID ="1690gbng";
    
    private static LambdaService lambdaService;
    
    @BeforeClass
    public static void createInput() throws IOException {
        invalidWebhook = new RequestClass(INVALID_SIGNATURE, INVALID_PAYLOAD);       
    }
    
    @BeforeClass
    public static void createLambdaServiceImpl() {
        lambdaService = new LambdaService();      
    }

    private static Context createContext() {
        TestContext ctx = new TestContext();
        ctx.setFunctionName(LambdaService.GREETING_MESSAGE);
        return ctx;
    }
    
    @Test
    public void testRequestClass(){
        Assert.assertTrue(invalidWebhook.getBt_signature().equals(INVALID_SIGNATURE));
        Assert.assertTrue(invalidWebhook.getBt_payload().equals(INVALID_PAYLOAD));
    }
    
    @Test
    public void testResponseClass(){
        ResponseClass responseClass = new ResponseClass(LambdaService.GREETING_MESSAGE);        
        Assert.assertTrue(responseClass.getGreetings().equals(LambdaService.GREETING_MESSAGE));
    }
    
    @Test(expected=NullPointerException.class)
    public void testReadWrongNamePropertiesFile() {
        Properties p = lambdaService.getProperties(wrongFileName);  
        Assert.assertNull(p);
    }
    
    @Test
    public void testReadEmptyProperties() {
        Properties p = lambdaService.getProperties(emptyFileName);       
        Assert.assertNotNull(p);
        Assert.assertNull(p.getProperty(URL));       
    }
        
    @Test
    public void testGetPropertiesAndReadProperties() {
        Properties p = lambdaService.getProperties(testFileName);       
        Assert.assertNotNull(p);
        Assert.assertTrue(p.getProperty(URL).equals(URL_VALUE));
        Assert.assertTrue(p.getProperty(USER_NAME).equals(USER_NAME_VALUE));
        Assert.assertTrue(p.getProperty(PASSWORD).equals(PASSWORD_VALUE));
        Assert.assertTrue(p.getProperty(DRIVER).equals(DRIVER_VALUE));
        Assert.assertTrue(p.getProperty(BRAINTREE_ENVIROMENT).equals(BRAINTREE_ENVIROMENT_VALUE));
        Assert.assertTrue(p.getProperty(BRAINTREE_MERCHANTID).equals(BRAINTREE_MERCHANTID_VALUE));
        Assert.assertTrue(p.getProperty(BRAINTREE_PUBLIC_KEY).equals(BRAINTREE_PUBLIC_KEY_VALUE));
        Assert.assertTrue(p.getProperty(BRAINTREE_PRIVATE_KEY).equals(BRAINTREE_PRIVATE_KEY_VALUE));
        Assert.assertTrue(p.getProperty(BRAINTREE_PUBLIC_KEY).equals(BRAINTREE_PUBLIC_KEY_VALUE));       
        
    }
    
    @Test 
    public void testGetDBConnection() {
        Connection conn = lambdaService.getDBconnection(testFileName);       
        Assert.assertNotNull(conn);
    }
    
    @Test
    public void testGetBraintreeGateway() {
        BraintreeGateway gateway = lambdaService.getBraintreeGateway(testFileName);       
        Assert.assertNotNull(gateway);
    }
    
    @Test
    public void testAcceptInvalidWebhooks() {      
        AcceptWebhooks handler = new AcceptWebhooks();
        Context ctx = createContext();        
        ResponseClass output = handler.handleRequest(invalidWebhook, ctx);
        
        Assert.assertTrue(output.getGreetings().equals(LambdaService.NULL_WEBHOOK_NOTIFICATION_MESSAGE));       
       
    }   
    
    @Test
    public void testNotHandledWebhooks() { 
        BraintreeGateway gateway = lambdaService.getBraintreeGateway(testFileName);
        HashMap<String, String> sampleNotification = gateway.webhookTesting().sampleNotification(
            WebhookNotification.Kind.SUBSCRIPTION_WENT_PAST_DUE, "my_id"
        );
        
        notHandleWebhook = new RequestClass(sampleNotification.get("bt_signature"), sampleNotification.get("bt_payload"));
        AcceptWebhooks handler = new AcceptWebhooks();
        Context ctx = createContext();        
        ResponseClass output = handler.handleRequest(notHandleWebhook, ctx);
        
        Assert.assertTrue(output.getGreetings().equals(LambdaService.NOT_HANDLE_WEBHOOK_NOTIFICATION_MESSAGE));    
    }
   
    
    @Test
    public void testcreateOrderAndOrderDetail() {
        BraintreeGateway gateway = lambdaService.getBraintreeGateway(testFileName);
        Transaction transaction = gateway.transaction().find(BRAINTREE_TRANSACTION_ID);
        
        Context ctx = createContext(); 
        
        String resultString = lambdaService.createOrderAndOrderDetail(BRAINTREE_ID, transaction, ctx.getLogger(), true);      
        Assert.assertTrue(resultString.equals(LambdaService.SUCCEED_INSERT_ORDER_AND_ORDERDETAIL_MESSAGE));
    }   
    
    @AfterClass
    public static void deleteOrderAndOrderDetailCreatedByTest() {
        Context ctx = createContext(); 
        String resultString = lambdaService.deleteOrderAndOrderDetail(ctx.getLogger());
        Assert.assertTrue(resultString.equals(LambdaService.SUCCEED_DELETE_ORDER_AND_ORDERDETAIL_MESSAGE));
    }
    
}
