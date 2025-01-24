import { Customer, TransactionHistory } from "../models";
import { CustomerRepository } from "../repositories";
import stripeClient from "../services/stripeClient";
// Constants for currency and description
const CURRENCY = "USD";
const INVOICE_DESCRIPTION = "Invoice item description";

// Function to delete pending invoice items
const deletePendingInvoiceItems = async () => {
  // Retrieve a list of all invoice items
  const invoiceItems = await stripeClient.invoiceItems.list();

  // Loop through the list and delete pending invoice items
  for (const item of invoiceItems.data) {
    if (!item.invoice) {
      await stripeClient.invoiceItems.del(item.id);
      console.log("Deleted pending invoice item:", item.id);
    }
  }
};

// Function to create invoice items and invoice
const createInvoiceItemsAndInvoice = async (
  stripeCustomerId: string,
  totalAmount: number,
  metaData: any,
  invoiceItemDescription: string
) => {
  // Create invoice items for the invoice
  await stripeClient.invoiceItems.create({
    customer: stripeCustomerId,
    amount: Math.floor(totalAmount * 100), // to convert in cents
    metadata: metaData,
    currency: CURRENCY,
    description: invoiceItemDescription || INVOICE_DESCRIPTION,
  });

  // Create the actual invoice
  const invoiceCreation = await stripeClient.invoices.create({
    customer: stripeCustomerId,
    metadata: metaData,
    collection_method: 'send_invoice', // Set collection method to 'send_invoice'
    days_until_due: 1, // Optionally set the days until the invoice is due

  });

  return invoiceCreation;
};

// Function to update customer credits
const updateCustomerCredits = async (
  customerData: any,
  noOfCredits: number,
  customerRepository: any,
  sourceType: string
) => {
  // for lead sorting
  if (sourceType == 'lead_sorting')
    customerData.row_credits = (customerData.row_credits ? customerData.row_credits : 0) + noOfCredits;
  else
    // for lead generation
    customerData.lead_gen_row_credits = (customerData.lead_gen_row_credits || 0) + noOfCredits;

  // Update the customer's credits
  await customerRepository.update(customerData);
};

// Main function for processing Stripe payment
export const stripePayment = async (
  args: any,
  customerData: any,
  customerRepository: any,
  transactionHistoryRepository: any,
  adminEventsRepository: any,
) => {
  const { totalAmount, metaData, noOfCredits, email, payment_type = "Subscription", invoiceItemDescription, sourceType,
    leadsToAdd = true, failedTransactionData } = args;
  const newMetaData = { ...metaData, payment_type }
  let invoiceId;
  let paidInvoiceData;
  let msg = "Credits purchased successfully";
  let data = {};
  let statusCode = 200;

  try {
    // Delete any pending invoice items before starting payment process
    await deletePendingInvoiceItems();

    // Retrieve the customer's Stripe ID
    const stripeCustomerId: string = customerData.stripe_customer_id;

    // Create invoice items and the invoice itself
    let invoiceCreation = null
    if (failedTransactionData?.invoice_id) {
      invoiceId = failedTransactionData?.invoice_id;
    } else {
      invoiceCreation = await createInvoiceItemsAndInvoice(
        stripeCustomerId,
        totalAmount,
        newMetaData,
        invoiceItemDescription
      );
      invoiceId = invoiceCreation.id;
    }

    if (invoiceId) {
      // Pay the created invoice
      paidInvoiceData = await stripeClient.invoices.pay(invoiceId);
      data = paidInvoiceData;

      if (!paidInvoiceData.paid) {
        statusCode = 402;
        msg = "Stripe payment failed, Please try again";
      } else if (noOfCredits && leadsToAdd) {
        // Update customer's credits if payment is successful
        await updateCustomerCredits(customerData, noOfCredits, customerRepository, sourceType);
      }
    } else {
      statusCode = 402;
      msg = "Invoice creation failed, Please try again";
    }

    if (failedTransactionData) {
      const updatedTransactionHistory = new TransactionHistory({
        ...args.failedTransitionData,
        invoice_id: invoiceId,
        invoice_amount: totalAmount,
        invoice_url: paidInvoiceData?.hosted_invoice_url,
        email,
        invoice_pdf: paidInvoiceData?.invoice_pdf,
        meta_data: metaData,
        error: statusCode !== 200,
        error_detail: msg,
        payment_type: payment_type
      });

      await Promise.all([
        transactionHistoryRepository.updateById(failedTransactionData.id, updatedTransactionHistory),
        adminEventsRepository.create({
          admin: "n/a",
          date: new Date(),
          user: email,
          num_credits: noOfCredits,
          remark: payment_type
        }),
      ]);
    } else {
      await Promise.all([
        transactionHistoryRepository.create({
          invoice_id: invoiceId,
          invoice_amount: totalAmount,
          invoice_url: paidInvoiceData?.hosted_invoice_url,
          email,
          invoice_pdf: paidInvoiceData?.invoice_pdf,
          meta_data: metaData,
          error: statusCode !== 200,
          error_detail: msg,
          payment_type: payment_type
        }),
        adminEventsRepository.create({
          admin: "n/a",
          date: new Date(),
          user: email,
          num_credits: noOfCredits,
          remark: payment_type
        }),
      ]);
    }

    // Add entries to transaction history and admin events

    await stripeClient.invoices.sendInvoice(paidInvoiceData.id);

    // Return response data
    return { data, msg, statusCode };
  } catch (error) {
    console.error("An error occurred:", error);
    if (failedTransactionData) {
      const updatedTransactionHistory = new TransactionHistory({
        ...args.failedTransitionData,
        invoice_id: invoiceId,
        invoice_amount: totalAmount,
        invoice_url: paidInvoiceData?.hosted_invoice_url,
        email,
        invoice_pdf: paidInvoiceData?.invoice_pdf,
        meta_data: metaData,
        error: true,
        error_detail: msg,
        payment_type: payment_type
      });

      await Promise.all([
        transactionHistoryRepository.updateById(failedTransactionData.id, updatedTransactionHistory),
        adminEventsRepository.create({
          admin: "n/a",
          date: new Date(),
          user: email,
          num_credits: noOfCredits,
          remark: payment_type
        }),
      ]);
    } else {
      await Promise.all([
        transactionHistoryRepository.create({
          invoice_id: invoiceId,
          invoice_amount: totalAmount,
          invoice_url: paidInvoiceData?.hosted_invoice_url,
          email,
          invoice_pdf: paidInvoiceData?.invoice_pdf,
          meta_data: metaData,
          error: true,
          error_detail: msg,
          payment_type: payment_type
        }),
        adminEventsRepository.create({
          admin: "n/a",
          date: new Date(),
          user: email,
          num_credits: noOfCredits,
          remark: payment_type
        }),
      ]);
    }

    // Set status code and error message for error response
    const statusCode = 500; // Internal Server Error
    const errorMsg = `An error occurred during payment processing - ${error?.message}`;

    return { data, msg: errorMsg, statusCode };
  }
};

export const changeSubscriptionPlan = async (customerId: string, newPlanId: string, startImmediately: boolean) => {
  try {
    // Retrieve the current subscription
    const customer = await stripeClient.customers.retrieve(customerId);
    //   const currentSubscriptionId = customer.subscriptions.data[0].id;
    const currentSubscriptionId = await getCurrentSubscriptionId(customerId);
    const currentSubscriptionItemId = await getCurrentSubscriptionItemId(customerId, currentSubscriptionId);

    // Calculate the proration if needed  
    const prorationBehavior = startImmediately ? 'create_prorations' : 'none';

    // Update the subscription
    const updatedSubscription = await stripeClient.subscriptions.update(currentSubscriptionId, {
      items: [{ id: currentSubscriptionItemId, plan: newPlanId }],
      proration_behavior: 'always_invoice',
    });
    //   const invoice = await stripeClient.invoices.retrieveUpcoming({
    //     customer: customerId,
    //   });
    // Handle invoices if proration is involved
    if (startImmediately && updatedSubscription.latest_invoice) {
      const invoice = await stripeClient.invoices.retrieve(updatedSubscription.latest_invoice);
      if (invoice.status !== 'paid') {
        // Only pay the invoice if it's not already paid
        await stripeClient.invoices.pay(updatedSubscription.latest_invoice);
      }
    }
    return { data: updatedSubscription, status: 200, msg: 'Plan Changed Successfully', success: true };
  } catch (error) {
    console.error('Error changing subscription plan:', error.message);
    return { data: {}, status: 400, msg: error.message, success: false };
  }
}

async function getCurrentSubscriptionId(customerId: string) {
  try {
    const subscriptions = await stripeClient.subscriptions.list({
      customer: customerId,
      status: 'active', // Optionally filter for active subscriptions
    });

    if (subscriptions.data.length > 0) {
      // Assuming you want the ID of the first active subscription
      return subscriptions.data[0].id;
    } else {
      // Handle the case when there's no active subscription
      return null;
    }
  } catch (error) {
    console.error('Error retrieving current subscription:', error.message);
    return null;
  }
}


const getCurrentSubscriptionItemId = async (customerId: string, subscriptionId: string) => {
  try {
    // Retrieve the current subscription and its associated items
    const subscription = await stripeClient.subscriptions.retrieve(subscriptionId, {
      expand: ['items.data.price'], // This expands the subscription items with price information
    });

    // Check if there are subscription items
    if (subscription.items && subscription.items.data.length > 0) {
      // Assuming you want the ID of the first subscription item
      return subscription.items.data[0].id;
    } else {
      console.log('No subscription items found in the current subscription.');
      return null;
    }
  } catch (error) {
    console.error('Error getting currentSubscriptionItemId:', error.message);
    return null;
  }
}

export const stripPaymentInfo = async (customerData: any) => {

  let cardInfo: any;
  const stripPaymentData = await stripeClient.paymentMethods.list({
    customer: customerData?.stripe_customer_id,
    type: "card", // This specifies that you want card payment methods
  });

  if (stripPaymentData) {
    const cardPaymentMethod = stripPaymentData.data[0];
    if (cardPaymentMethod) {
      // Access the last 4 digits of the card
      cardInfo = cardPaymentMethod.card;
      // console.log("Last 4 digits of the card: " + last4);
    } else {
      console.log("No card payment method found for this customer.");
    }
  }
  return { cardInfo }


}

export const updateStripeDefaultPayment = async (
  findbyemail: any,
  stripePaymentMethodId: string,
  customerRepository: any,
) => {
  let msg = "payment method added successfully";
  let data = {};
  let statusCode = 200;

  try {
    if (stripePaymentMethodId) {
      if (!findbyemail.pricing_plan) {
        findbyemail.pricing_plan = {
          stripe_subscription_status: "",
          subscription_id: "",
          current_period_end: 0,
          plan: null,
          stripe_price_id: "",
          stripe_product_id: "",
          stripe_payment_method_id: stripePaymentMethodId,
          stripe_invoice_id: "",
          stripe_payment_intent_status: "",
          stripe_payment_intent_client_secret: "",
          resume_paused_subscription_at: null,
          start_paused_subscription_at: null,
        };
      }
      else {
        findbyemail.pricing_plan.stripe_payment_method_id = stripePaymentMethodId;
      }

      await stripeClient.paymentMethods.attach(stripePaymentMethodId, {
        customer: findbyemail.stripe_customer_id,
      });
      await Promise.all([
        customerRepository.update(findbyemail),
        stripeClient.customers.update(
          findbyemail.stripe_customer_id,
          {
            invoice_settings: {
              default_payment_method: stripePaymentMethodId,
            },
          }
        )]);
    }
    // update all child account with pricing plan
    await updateChildAccountPricingPlan(findbyemail, customerRepository);
    // Return response data
    return { data, msg, statusCode };
  } catch (error) {
    console.error("An error occurred:", error);

    // Set status code and error message for error response
    const statusCode = 500; // Internal Server Error
    const errorMsg = `An error occurred during attaching default method - ${error?.message}`;

    return { data, msg: errorMsg, statusCode };
  }
};

export const updateChildAccountPricingPlan = async (parentObj: Customer, customerRepository: CustomerRepository) => {

  try {

    const childAccounts = await customerRepository.updateAll({ pricing_plan: parentObj.pricing_plan }, {
      parent_email: parentObj.email
    })

  }
  catch (error) {
    console.error("Error in updating pricing plan for child accounts", error)
  }
  return
}