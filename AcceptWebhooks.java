package com.yanping.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import com.braintreegateway.Transaction;
import com.braintreegateway.WebhookNotification;


public class AcceptWebhooks implements RequestHandler<RequestClass, ResponseClass> {
    
    @Override
    public ResponseClass handleRequest(RequestClass request, Context context) {
        
        LambdaLogger logger = context.getLogger();             
        
        String greetingString = LambdaService.GREETING_MESSAGE;
        String resultString = LambdaService.SUCCESS_WEBHOOD_NOTIFICATION_HANDLE_MESSAGE;
        
        logger.log(greetingString);
        
        LambdaService lambdaService = new LambdaService();
           
        WebhookNotification webhookNotification = lambdaService.getWebhookNotification(request, logger);
        
        if (webhookNotification != null) {
            logger.log("[Webhook Received " + webhookNotification.getTimestamp().getTime() + "] | Kind: " 
                + webhookNotification.getKind() + " | Subscription: " + webhookNotification.getSubscription().getId()
                + " | Current billing cycle: " + webhookNotification.getSubscription().getCurrentBillingCycle());
        }
        else {
            resultString = LambdaService.NULL_WEBHOOK_NOTIFICATION_MESSAGE;
        }
        
        //Braintree guarantees that kind and subscription will be non-null for recurring billing webhooks.
        if (webhookNotification != null &&               
            webhookNotification.getKind().equals(WebhookNotification.Kind.SUBSCRIPTION_CHARGED_SUCCESSFULLY) &&
            webhookNotification.getSubscription().getCurrentBillingCycle() > 1){
            
            String braintreeSubscriptionId = webhookNotification.getSubscription().getId();
            Transaction transaction = webhookNotification.getSubscription().getTransactions().get(0);
            
            logger.log("Create order and order detail in bonfire database");
            resultString = lambdaService.createOrderAndOrderDetail(braintreeSubscriptionId, transaction, logger, false);
            
            if (resultString.equals(LambdaService.SUCCEED_INSERT_ORDER_AND_ORDERDETAIL_MESSAGE)) {
                resultString = LambdaService.SUCCESS_WEBHOOD_NOTIFICATION_HANDLE_MESSAGE;
            }
        
        }
        else if (webhookNotification != null) {
            resultString = LambdaService.NOT_HANDLE_WEBHOOK_NOTIFICATION_MESSAGE;
        }
               
        logger.log(resultString);
        return new ResponseClass(resultString);
    }

}
