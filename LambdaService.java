package com.yanping.lambda;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.braintreegateway.BraintreeGateway;
import com.braintreegateway.Transaction;
import com.braintreegateway.WebhookNotification;
import com.braintreegateway.exceptions.InvalidSignatureException;

public class LambdaService {
    // Attach this number to MerchantReferenceCode to make its format be consistent with 
    // MerchantReferenceCode created in bonfire
    public static final int PLACEHOLDER_NUM = 1;
    
    public static final DateTimeFormatter ORDER_DATE_FMT =DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withLocale( Locale.US )
            .withZone( ZoneId.of("America/Los_Angeles"));
   
    public static final DateTimeFormatter INSERTED_DATE_FMT =DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withLocale( Locale.US )
            .withZone( ZoneId.of("America/Los_Angeles"));
    
    public static final DateTimeFormatter MRC_FMT =DateTimeFormatter.ofPattern("yyMMddHHmmssSSS")
            .withLocale( Locale.US )
            .withZone( ZoneId.of("America/Los_Angeles"));
    
    public static final  String ORDER_STATUS = "PAID";
    public static final  String PR_STATUS = "REGISTERED";
    
    public final String propertiesFileName = "./candles.properties";
    
    public static final String GREETING_MESSAGE = "Accepting Braintree Webhooks";
    public static final String NULL_WEBHOOK_NOTIFICATION_MESSAGE = "Null Webhook Notification";
    public static final String NOT_HANDLE_WEBHOOK_NOTIFICATION_MESSAGE ="No need to do anything for this kind of Webhook Notification";
    public static final String SUCCESS_WEBHOOD_NOTIFICATION_HANDLE_MESSAGE = "The Webhook Notification is handled successfully"; 
    public static final String FAILED_INSERT_ORDER_AND_ORDERDETAIL_MESSAGE = "Failed to insert new order and order detail into the database";
    public static final String SUCCEED_INSERT_ORDER_AND_ORDERDETAIL_MESSAGE = "Succeed to insert new order and order detail into the database";
    public static final String FAILED_DELETE_ORDER_AND_ORDERDETAIL_MESSAGE = "Failed to delete order and order detail that inserted by test";
    public static final String SUCCEED_DELETE_ORDER_AND_ORDERDETAIL_MESSAGE = "Succeed to delete order and order detail that inserted by test";
    public static final String TEST_MERCHANTREFERENCE_CODE = "we-0000000000000000-tttttttt";
    
    /**
     * Get the database connection
     * @param property file name
     * @return database connection
     */
    public Connection getDBconnection(String fileName){
        Connection dbConnection = null;
        Properties properties = getProperties(fileName);
        
        final String url = properties.getProperty("database.url");
        final String userName = properties.getProperty("database.username");
        final String password = properties.getProperty("database.password");
        final String driver = properties.getProperty("database.driverClassName");
        
        try {
            Class.forName(driver);
            dbConnection = DriverManager.getConnection(url, userName, password);
  
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }        
        return dbConnection;        
    }
    
    /**
     * Get properties from system property file
     * @param property fileName
     * @return
     */
    public Properties getProperties(String fileName){
        Properties properties = new Properties();
        InputStream propertyStream = null;
      
        try {
            propertyStream = getClass().getClassLoader().getResource(fileName).openStream();
            properties.load(propertyStream);
        } catch (IOException e2) {
            e2.printStackTrace();
        } finally {
            try {
                if (propertyStream != null)
                    propertyStream.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }     
        return properties;       
     }
      
    /**
     * Get Braintree Gateway
     * @param property file name
     * @return Braintree Gateway
     */
    public BraintreeGateway getBraintreeGateway(String fileName){
        Properties properties = getProperties(fileName);
        
        final String braintreeEnvironment = properties.getProperty("braintreeEnvironment");
        final String braintreeMerchantID = properties.getProperty("braintreeMerchantID");
        final String braintreePublicKey = properties.getProperty("braintreePublicKey");
        final String braintreePrivateKey = properties.getProperty("braintreePrivateKey");
        
        BraintreeGateway gateway = new BraintreeGateway(braintreeEnvironment, braintreeMerchantID, braintreePublicKey,
                braintreePrivateKey);
        
        return gateway;
    }
    
    /**
     * Create new order and order details in bonfire database
     * @param Braintree subscription Id
     * @param Braintree transaction
     * @param LambdaLogger
     * @param notTest: true, create a real order and order detail records; false, create a test order and order detail records.
     */
    @SuppressWarnings("resource")
    public String createOrderAndOrderDetail(String braintreeSubscriptionId, Transaction transaction, LambdaLogger logger, boolean isTest) {
        String resultString = SUCCEED_INSERT_ORDER_AND_ORDERDETAIL_MESSAGE;
        Connection dbConnection = getDBconnection(propertiesFileName);         
        
        PreparedStatement stInsertOrder = null;
        PreparedStatement stInsertOrderDetail = null;
        
        long orderId = 0L;
        long customerId = 0L;
        String cltmMaskedCreditCardNumber = null;
        int cltmPaymentMethod = 0;
        int cltmDivision = 0;
        int cltmCreditCardType = 0;
        String cltmMerchantReferenceCode = null;
        
        try {               
            // query subscription id (braintree_id) in braintree_subscription table to get order_id
            String getOrderIdQuery = "Select order_id from braintree_subscription where braintree_id='" + braintreeSubscriptionId + "'";           
            PreparedStatement stOrderId = dbConnection.prepareStatement(getOrderIdQuery);
            ResultSet rsOrderId = stOrderId.executeQuery();
        
            // iterate through the java result set
            while (rsOrderId.next())
            {
                orderId = rsOrderId.getLong("order_id");
                logger.log("Bonfire order id of first billing cycle of the subscription is: " + orderId);
           
                String getOrderQuery = "Select * from order_header where id=" + orderId;
                PreparedStatement stOrder = dbConnection.prepareStatement(getOrderQuery);               
                ResultSet rsOrder = stOrder.executeQuery();
                while(rsOrder.next()){
                    customerId = rsOrder.getLong("customer_id");
                    
                    //info will be used for new order
                    cltmDivision = rsOrder.getInt("cltm_division");
                    cltmMaskedCreditCardNumber = rsOrder.getString("cltm_maskedcreditcardnumber");
                    cltmPaymentMethod = rsOrder.getInt("cltm_paymentMethod");
                    cltmCreditCardType = rsOrder.getInt("cltm_creditCardType");
                    cltmMerchantReferenceCode = rsOrder.getString("cltm_merchantreferencecode");
                    logger.log("[ customer id: " + customerId + 
                            " | cltm division: " + cltmDivision +
                            " | cltm masked credit card number: " + cltmMaskedCreditCardNumber +
                            " | cltm payment method: " + cltmPaymentMethod +
                            " | cltm credit card type: " + cltmCreditCardType +
                            " | cltm merchant reference code: " + cltmMerchantReferenceCode
                            );                 
                }
                stOrder.close();              
            }            
            stOrderId.close();
        } catch (SQLException e) {
            logger.log("Got an exception when try to get informations from order-header table! " + e.getMessage());
        }    
        
        String billingAddress = null;
        String billingAddress2 = null;
        String billingCity = null;
        String billingState = null;
        String billingZip = null;
        String billingCountry = null;
        String shippingAddress = null;
        String shippingAddress2 = null;
        String shippingCity = null;
        String shippingState = null;
        String shippingZip = null;
        String shippingCountry = null;

        try{               
            // get address info from bonfire customer table
            String getCustomerQuery = "Select * from customer where id=" + customerId;              
            PreparedStatement stCustomer = dbConnection.prepareStatement(getCustomerQuery);
            ResultSet rsCustomer = stCustomer.executeQuery();
        
            while (rsCustomer.next())
            {
                billingAddress = rsCustomer.getString("billing_address");                 
                billingAddress2 = rsCustomer.getString("billing_address_2");
                billingCity = rsCustomer.getString("billing_city");
                billingState = rsCustomer.getString("billing_state");
                Long billingCountryId = rsCustomer.getLong("billing_country_id");
                billingZip = rsCustomer.getString("billing_zip");
                
                String getBillCountryCodeQuery = "Select code from country where id=" + billingCountryId;              
                PreparedStatement stBillCountryCode = dbConnection.prepareStatement(getBillCountryCodeQuery);
                ResultSet rsBillCountryCode = stBillCountryCode.executeQuery();
                while (rsBillCountryCode.next()){
                    billingCountry = rsBillCountryCode.getString("code");
                }
                stBillCountryCode.close();
                
                shippingAddress = rsCustomer.getString("shipping_address");                 
                shippingAddress2 = rsCustomer.getString("shipping_address_2");
                shippingCity = rsCustomer.getString("shipping_city");
                shippingState = rsCustomer.getString("shipping_state");
                Long shippingCountryId = rsCustomer.getLong("shipping_country_id");
                shippingZip = rsCustomer.getString("shipping_zip");
                
                String getShipCountryCodeQuery = "Select code from country where id=" + shippingCountryId;              
                PreparedStatement stShipCountryCode = dbConnection.prepareStatement(getShipCountryCodeQuery);
                ResultSet rsShipCountryCode = stShipCountryCode.executeQuery();
                while (rsShipCountryCode.next()){
                    shippingCountry = rsShipCountryCode.getString("code");
                }
                stShipCountryCode.close();
            }
            stCustomer.close();
        } catch (SQLException e) {
            logger.log("Got an exception when try to get billing and shipping address from cusmtomer table! " + e.getMessage());
        }      
            
        
        int lineNo = 0;
        Long systemId = null;
        Long skuId = null;
        BigDecimal pricePerUnit = null;
        
        try {
            //get the previous order detail
            String getOrderDetailQuery = "Select * from order_detail where order_id=" + orderId;              
            PreparedStatement stOrderDetail = dbConnection.prepareStatement(getOrderDetailQuery);
            ResultSet rsOrderDetail = stOrderDetail.executeQuery();              
                          
            while (rsOrderDetail.next()){
                lineNo = rsOrderDetail.getInt("line_no");
                systemId = rsOrderDetail.getLong("system_id");
                skuId = rsOrderDetail.getLong("sku_id");
                pricePerUnit = rsOrderDetail.getBigDecimal("priceperunit");
            }
            stOrderDetail.close();
        } catch (SQLException e) {
            logger.log("Got an exception when try to get informations from order_detail table! " + e.getMessage());
        }                  
        
        logger.log("transaction id is :" +  transaction.getId());
            
        String firstName = transaction.getCustomer().getFirstName();
        String lastName = transaction.getCustomer().getLastName();
        String email = transaction.getCustomer().getEmail();
        BigDecimal amount = transaction.getAmount();
        Calendar orderDate = transaction.getCreatedAt();
            
        //create new cltm_merchantreferencecode for the new order              
        String newCltmMerchantReferenceCode = cltmMerchantReferenceCode.substring(0,3) + MRC_FMT.format(orderDate.toInstant()) + PLACEHOLDER_NUM + "-"+ transaction.getId() ;       
        
        //for test purpose
        if (isTest) {
            newCltmMerchantReferenceCode = TEST_MERCHANTREFERENCE_CODE;   
        }
        
        logger.log("new cltm merchant reference code is " + newCltmMerchantReferenceCode + " and insert a new order in order_header table with it");
                           
        java.sql.Timestamp insertedDate = new Timestamp(System.currentTimeMillis());
            
        String insertOrderQuery = "INSERT INTO order_header (customer_id, shipto_line1, shipto_line2, shipto_city, shipto_stateorprovince, "
		        + "shipto_postalcode, shipto_country, cltm_billtofirstname, cltm_billtolastname, cltm_billtoemailaddress, billto_line1, billto_line2,"
				+ "billto_city, billto_stateorprovince, billto_postalcode, billto_country, cltm_creditcardtype, cltm_division, cltm_maskedcreditcardnumber, "
				+ "cltm_merchantreferencecode, cltm_paymentmethod, inserted_date, order_status, pr_status, order_date) VALUES (" 
                + customerId + ", '" + shippingAddress + "', '"+ shippingAddress2 + "', '" + shippingCity + "', '" + shippingState + "', '" + shippingZip + "', '" 
                + shippingCountry + "', '"+ firstName + "', '"+ lastName + "', '"+ email + "', '"+ billingAddress + "', '"+ billingAddress2 + "', '"+ billingCity + "', '"  
                + billingState + "', '"+ billingZip + "', '"+ billingCountry + "', "+ cltmCreditCardType+ ", "+ cltmDivision+ ", '"+ cltmMaskedCreditCardNumber+ "', '"
                + newCltmMerchantReferenceCode+ "', "+ cltmPaymentMethod+ ", '"+ INSERTED_DATE_FMT.format(insertedDate.toInstant()) + "', '" + ORDER_STATUS + "', '" + PR_STATUS + "', '" 
                + ORDER_DATE_FMT.format(orderDate.toInstant()) + "')";
            
        logger.log("insert order query: " + insertOrderQuery);
        
        try {
            dbConnection.setAutoCommit(false);
            stInsertOrder = dbConnection.prepareStatement(insertOrderQuery);
            //data IS NOT commit yet
            stInsertOrder.executeUpdate();
            
            //create new order detail              
            int quantity =  amount.divide(pricePerUnit).intValue();
            
            orderDate.set(Calendar.HOUR_OF_DAY, 0);
            orderDate.set(Calendar.MINUTE,  0);
            orderDate.set(Calendar.SECOND,  1);  
            Date maintStartDate= orderDate.getTime();                   
            Calendar calendarYearFromTransactionDate = (Calendar) orderDate.clone();
            calendarYearFromTransactionDate.add(Calendar.YEAR,  1);
            calendarYearFromTransactionDate.add(Calendar.DATE,  -1);
            Date maintEndDate = calendarYearFromTransactionDate.getTime();                   
            
            //get the order id of the order just inserted
            String getNewOrderIdQuery = "Select id from order_header where cltm_merchantreferencecode like '%" + newCltmMerchantReferenceCode + "'";              
            PreparedStatement stNewOrderId = dbConnection.prepareStatement(getNewOrderIdQuery);
            ResultSet rsNewOrderId = stNewOrderId.executeQuery();
            Long newOrderId = null;
            while (rsNewOrderId.next()){
                newOrderId = rsNewOrderId.getLong("id");
            }
            
            String insertOrderDetailQuery = "INSERT INTO order_detail (order_id, line_no, system_id, maint_start_date, maint_end_date, sku_id, priceperunit, quantity, inserted_date) VALUES (" 
                    + newOrderId + ", "  + lineNo + ", " + systemId + ", '"+ LambdaService.INSERTED_DATE_FMT.format(maintStartDate.toInstant()) + "', '" 
                    + LambdaService.INSERTED_DATE_FMT.format(maintEndDate.toInstant()) + "', " + skuId + ", " + pricePerUnit + ", " + quantity + ", '"
                    + LambdaService.INSERTED_DATE_FMT.format(insertedDate.toInstant()) + "')";
                    
           logger.log("insert order detail query: " + insertOrderDetailQuery); 
           
           stInsertOrderDetail = dbConnection.prepareStatement(insertOrderDetailQuery);
           stInsertOrderDetail.executeUpdate();      
           
           //transaction block end. if there is erros, rollback, including the order insert PreparedStatement.
           dbConnection.commit(); 
           
        } catch (SQLException e) {
            logger.log("Got an exception when try to insert new order and new order detail! " + e.getMessage());
            resultString = FAILED_INSERT_ORDER_AND_ORDERDETAIL_MESSAGE;
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                logger.log("Failed to do database roll back");
            }
        } finally {
            if (stInsertOrder != null) {
                try {
                    stInsertOrder.close();
                } catch (SQLException e) {
                    logger.log("Failed to close insert order statement");
                }
            }

            if (stInsertOrderDetail != null) {
                try {
                    stInsertOrderDetail.close();
                } catch (SQLException e) {
                    logger.log("Failed to close insert order detail statement");
                }
            }

            if (dbConnection != null) {
                try {
                    dbConnection.close();
                } catch (SQLException e) {
                    logger.log("Failed to close database connection");
                }
            }            
        }
        
        return resultString;
        
    }
    
    /**
     * Get Braintree Webhook notification
     * @param request
     * @param logger
     * @return
     */
    public WebhookNotification getWebhookNotification(RequestClass request, LambdaLogger logger){
        BraintreeGateway gateway = getBraintreeGateway(propertiesFileName);
        
        WebhookNotification webhookNotification = null;
        try {
            webhookNotification = gateway.webhookNotification().parse(
                request.getBt_signature(), 
                request.getBt_payload()              
            ); 
        } catch(InvalidSignatureException e){
            logger.log("InvalidSignatureException " + e);
            webhookNotification = null;
        }
        
        return webhookNotification;
    }
    
    /**
     * Used for test, clean up the database records that inserted by junit test
     */
    public String deleteOrderAndOrderDetail(LambdaLogger logger){
        String resultString = SUCCEED_DELETE_ORDER_AND_ORDERDETAIL_MESSAGE;
        Connection dbConnection = getDBconnection(propertiesFileName);         
        
        PreparedStatement stDeleteOrder = null;
        PreparedStatement stDeleteOrderDetail = null;
        PreparedStatement stGetOrderId = null;
        Long orderId = null;
        
        String getOrderIdQuery = "Select id from order_header where cltm_merchantreferencecode like '%" + TEST_MERCHANTREFERENCE_CODE + "'";
        try {
            stGetOrderId = dbConnection.prepareStatement(getOrderIdQuery);
            ResultSet rsOrderId = stGetOrderId.executeQuery();              
                          
            while (rsOrderId.next()){
                orderId = rsOrderId.getLong("id");
            }
            stGetOrderId.close();
            
        } catch (SQLException e) {
            logger.log("Got an exception when try to get order id from order_header table! " + e.getMessage());
        } 
        
        String deleteOrderDetailQuery = "Delete from order_detail where order_id=" + orderId;
        String deleteOrderQuery = "Delete from order_header where id =" + orderId;
        
        try {
            dbConnection.setAutoCommit(false);
            stDeleteOrderDetail = dbConnection.prepareStatement(deleteOrderDetailQuery);
            stDeleteOrder = dbConnection.prepareStatement(deleteOrderQuery);
            stDeleteOrderDetail.executeUpdate();
            stDeleteOrder.executeUpdate();
            dbConnection.commit();           
        } catch (SQLException e) {
            logger.log("Got an exception when try to delete order and order detail that inserted by test " + e.getMessage());
            resultString = FAILED_DELETE_ORDER_AND_ORDERDETAIL_MESSAGE;
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                logger.log("Failed to do database roll back");
            }
        } finally {
            if (stDeleteOrderDetail != null) {
                try {
                    stDeleteOrderDetail.close();
                } catch (SQLException e) {
                    logger.log("Failed to close delete order detail statement");
                }
            }

            if (stDeleteOrder != null) {
                try {
                    stDeleteOrder.close();
                } catch (SQLException e) {
                    logger.log("Failed to close delete order statement");
                }
            }

            if (dbConnection != null) {
                try {
                    dbConnection.close();
                } catch (SQLException e) {
                    logger.log("Failed to close database connection");
                }
            }            
        }
        
        return resultString;
    }    
    
}
